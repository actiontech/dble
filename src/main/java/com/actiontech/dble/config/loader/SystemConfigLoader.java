/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public final class SystemConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigLoader.class);
    private static final String BOOT_STRAP_FILE_NAME = "/bootstrap.cnf";
    private static final String BOOT_STRAP_DYNAMIC_FILE_NAME = "/bootstrap.dynamic.cnf";
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
                int ind = line.indexOf('=');
                if (ind < 0) {
                    continue;
                }
                String key = line.substring(0, ind).trim();
                String value = line.substring(ind + 1).trim();
                if (key.startsWith("-D") && !key.startsWith("-Dcom.sun.management.jmxremote")) {
                    pros.put(key.substring(2), value);
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

    private static Properties readBootStrapDynamicConf() throws IOException {
        Properties pros = new Properties();
        try (InputStream configIS = ResourceUtil.getResourceAsStream(BOOT_STRAP_DYNAMIC_FILE_NAME)) {
            if (configIS == null) {
                LOGGER.info(BOOT_STRAP_DYNAMIC_FILE_NAME + " is not exists");
                return pros;
            }
            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.warn("readBootStrapDynamicConf error:", e);
            throw e;
        }
        return pros;
    }

    public static void initSystemConfig() throws IOException, InvocationTargetException, IllegalAccessException {
        SystemConfig systemConfig = SystemConfig.getInstance();

        ParameterMapping.mapping(systemConfig, null);
        Properties system = new Properties();

        if (systemConfig.getInstanceId() == null) {
            // if not start with wrapper.conf
            system = readBootStrapConf();
        }

        Properties systemDynamic = readBootStrapDynamicConf();
        for (Object key : systemDynamic.keySet()) {
            system.put(key, systemDynamic.get(key));
        }
        ParameterMapping.mapping(systemConfig, system, null);
        if (system.size() > 0) {
            Set<String> propItem = new HashSet<>();
            for (Object key : system.keySet()) {
                String strKey = (String) key;
                if (!System.getProperties().keySet().contains(strKey)) {
                    propItem.add(strKey);
                }
            }
            if (propItem.size() > 0) {
                throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(propItem, ","));
            }
        }
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
                    }
                }
            }

            if (validVersion) {
                Versions.setServerVersion(systemConfig.getFakeMySQLVersion());
            } else {
                throw new ConfigException("The specified MySQL Version (" + systemConfig.getFakeMySQLVersion() + ") is not valid, " +
                        "the version should look like 'x.y.z'.");
            }
        }
        if (ClusterConfig.getInstance().isClusterEnable()) {
            LOGGER.info("use cluster, can not use simple python ha, so setUseOuterHa = true");
            systemConfig.setUseOuterHa(true);
        }
    }

}
