/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.config.util.StartProblemReporter;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.net.DbleSocketOptions;
import com.actiontech.dble.server.status.SqlDumpLog;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.SystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public final class SystemConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigLoader.class);
    private static final String BOOT_STRAP_FILE_NAME = "/bootstrap.cnf";
    public static final String BOOT_STRAP_DYNAMIC_FILE_NAME = "/bootstrap.dynamic.cnf";
    private static final String LOCAL_WRITE_PATH = "./";

    private SystemConfigLoader() {
    }

    private static Properties readBootStrapConf() throws IOException {
        Properties pros = new Properties();
        BufferedReader in = null;
        try (InputStream configIS = ResourceUtil.getResourceAsStream(BOOT_STRAP_FILE_NAME)) {
            if (configIS == null) {
                String msg = BOOT_STRAP_FILE_NAME + " is not exists";
                LOGGER.warn(msg);
                throw new IOException(msg);
            }
            in = new BufferedReader(new InputStreamReader(configIS));

            for (String line; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.length() == 0 || line.startsWith("#")) {
                    continue;
                }
                // only support these option
                if (line.startsWith("-server") || line.startsWith("-X") || line.startsWith("-agentlib") ||
                        line.startsWith("-javaagent") || line.startsWith("-Dcom.sun.management.jmxremote")) {
                    continue;
                }
                int ind = line.indexOf('=');
                if (ind < 0) {
                    throw new IOException("bootStrapConf format error:" + line);
                }
                String key = line.substring(0, ind).trim();
                String value = line.substring(ind + 1).trim();
                if (key.startsWith("-D")) {
                    pros.setProperty(key.substring(2), value);
                } else {
                    throw new IOException("bootStrapConf format error:" + line);
                }
            }
        } catch (IOException e) {
            LOGGER.error("readBootStrapConf error:", e);
            throw e;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                //ignore error
            }
        }
        return pros;
    }

    public static Properties readBootStrapDynamicConf() throws IOException {
        Properties pros = new Properties();
        try (InputStream configIS = ResourceUtil.getResourceAsStream(BOOT_STRAP_DYNAMIC_FILE_NAME)) {
            if (configIS == null) {
                //create
                String path = ResourceUtil.getResourcePathFromRoot(LOCAL_WRITE_PATH);
                path = new File(path).getPath() + BOOT_STRAP_DYNAMIC_FILE_NAME;
                FileUtils.createFile(path);
                LOGGER.info("create file: " + BOOT_STRAP_DYNAMIC_FILE_NAME);
            } else {
                pros.load(configIS);
            }
        } catch (IOException e) {
            LOGGER.warn("readBootStrapDynamicConf error:", e);
            throw e;
        }
        return pros;
    }

    public static void initSystemConfig() throws IOException, InvocationTargetException, IllegalAccessException {
        SystemConfig systemConfig = SystemConfig.getInstance();

        //-D properties
        Properties system = ParameterMapping.mappingFromSystemProperty(systemConfig, StartProblemReporter.getInstance());

        if (systemConfig.getInstanceName() == null) {
            // if not start with wrapper , usually for debug
            LOGGER.info("start without Java Service Wapper");
            system = readBootStrapConf();
        } else {
            Iterator<Object> iter = system.keySet().iterator();
            while (iter.hasNext()) {
                Object key = iter.next();
                String strKey = (String) key;
                if (strKey.startsWith("com.sun.management.jmxremote") || strKey.startsWith("wrapper.") ||
                        strKey.startsWith("java.rmi.")) {
                    iter.remove();
                }
            }
        }

        Properties systemDynamic = readBootStrapDynamicConf();
        system.putAll(systemDynamic);
        //must be first
        if (!StringUtil.isEmpty(system.getProperty("fakeMySQLVersion"))) {
            systemConfig.setFakeMySQLVersion(system.getProperty("fakeMySQLVersion"));
        }
        ParameterMapping.mapping(systemConfig, system, StartProblemReporter.getInstance());
        if (system.size() > 0) {
            Set<String> propItem = new HashSet<>();
            for (Object key : system.keySet()) {
                String strKey = (String) key;
                if (!SystemProperty.getInnerProperties().contains(strKey) &&
                        !SystemProperty.isJavaInnerProperties(strKey)) {
                    propItem.add(strKey);
                }
            }
            if (propItem.size() > 0) {
                StartProblemReporter.getInstance().addError("These properties in bootstrap.cnf or bootstrap.dynamic.cnf are not recognized: " + StringUtil.join(propItem, ","));
            }
        }
        postSet(systemConfig);
    }

    /**
     * do postCheck and postSetting here
     */
    private static void postSet(SystemConfig systemConfig) {
        final StartProblemReporter problemReporter = StartProblemReporter.getInstance();

        if (systemConfig.isUseDefaultPageNumber()) {
            systemConfig.setBufferPoolPageNumber((short) (Platform.getMaxDirectMemory() * 0.8 / systemConfig.getBufferPoolPageSize()));
        }
        if (systemConfig.getFakeMySQLVersion() != null) {
            boolean validVersion = false;
            String majorMySQLVersion = systemConfig.getFakeMySQLVersion();
            String[] versions = majorMySQLVersion.split("\\.");
            if (versions.length == 3) {
                majorMySQLVersion = versions[0] + "." + versions[1];
                for (String ver : SystemConfig.MYSQL_VERSIONS) {
                    // version is x.y.z ,just compare the x.y
                    if (majorMySQLVersion.equals(ver)) {
                        validVersion = true;
                        break;
                    }
                }
            }

            if (validVersion) {
                Versions.setServerVersion(systemConfig.getFakeMySQLVersion());
            } else {
                problemReporter.addError("The specified MySQL Version (" + systemConfig.getFakeMySQLVersion() + ") is not valid, " +
                        "the version should look like 'x.y.z'.");
            }
        }
        if (ClusterConfig.getInstance().isClusterEnable() && !systemConfig.isUseOuterHa()) {
            systemConfig.setUseOuterHa(true);
            LOGGER.warn("when use cluster mode, you can not use simple python ha, so dble will set useOuterHa=true to bootstrap.dynamic.cnf");
            try {
                WriteDynamicBootstrap.getInstance().changeValue("useOuterHa", "true");
            } catch (IOException e) {
                LOGGER.warn("setting useOuterHa=true to bootstrap.dynamic.cnf failed", e);
                problemReporter.addError("setting useOuterHa=true to bootstrap.dynamic.cnf failed");
            }
        }


        final Integer heapTableBufferChunkSize = systemConfig.getHeapTableBufferChunkSize();
        if (heapTableBufferChunkSize == null) {
            /*
                there are two case:
                1. the prop has been try to set before, but not set successfully.It causes an error. .
                2. the prop not set.
                we shouldn't set default value in case 1.
            */
            if (problemReporter.getErrorConfigs().isEmpty()) {
                LOGGER.info("Property [ heapTableBufferChunkSize ] in bootstrap.cnf is not set.The default value will the same as bufferPoolChunkSize");
                systemConfig.setHeapTableBufferChunkSize((int) systemConfig.getBufferPoolChunkSize());
            }
        } else if (heapTableBufferChunkSize <= 0 || heapTableBufferChunkSize % systemConfig.getBufferPoolChunkSize() != 0) {
            problemReporter.warn("Property [ heapTableBufferChunkSize ] '" + heapTableBufferChunkSize + "' in bootstrap.cnf is illegal, it must be a multiple of property 'bufferPoolChunkSize' and great than 0. ");
        }
    }


    public static void verifyOtherParam() throws IOException {
        // other
        SqlDumpLog.getInstance().verify();

        // base
        if (SystemConfig.getInstance().getInstanceName() == null) {
            StartProblemReporter.getInstance().addError("You must config instanceName in bootstrap.cnf and make sure it is an unique key for cluster");
        }
        String home = SystemConfig.getInstance().getHomePath();
        if (home == null) {
            StartProblemReporter.getInstance().addError("homePath is not set.");
        }
        DbleSocketOptions.getInstance().check();
    }
}
