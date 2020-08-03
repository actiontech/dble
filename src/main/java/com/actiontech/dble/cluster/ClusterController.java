/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Created by szf on 2018/2/28.
 */
public final class ClusterController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterController.class);

    public static final String CONFIG_FILE_NAME = "/cluster.cnf";
    public static final String CONFIG_MODE_UCORE = "ucore";
    public static final String CONFIG_MODE_USHARD = "ushard";
    public static final String CONFIG_MODE_ZK = "zk";
    public static final String CONFIG_MODE_CUSTOMIZATION = "customization";

    public static final int GRPC_SUBTIMEOUT = 70;
    public static final int GENERAL_GRPC_TIMEOUT = 10;

    private ClusterController() {
    }

    public static ClusterGeneralConfig init() {
        //read from cluster.cnf to tall use zk or ucore
        try {
            ClusterGeneralConfig clusterGeneralConfig = ClusterGeneralConfig.initConfig();
            ClusterGeneralConfig.initData();
            return clusterGeneralConfig;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void initFromShellUcore() throws InvocationTargetException, IllegalAccessException {
        loadClusterProperties();
        ClusterGeneralConfig.initConfig();
        ClusterGeneralConfig.getInstance().getClusterSender().initConInfo();
    }

    public static void initFromShellZK() throws InvocationTargetException, IllegalAccessException {
        loadClusterProperties();
        if (!CONFIG_MODE_ZK.equals(ClusterConfig.getInstance().getClusterMode())) {
            throw new RuntimeException("Cluster mode is not " + CONFIG_MODE_ZK);
        }
    }


    public static void loadClusterProperties() throws InvocationTargetException, IllegalAccessException {
        Properties pros = new Properties();

        try (InputStream configIS = ResourceUtil.getResourceAsStream(CONFIG_FILE_NAME)) {
            if (configIS == null) {
                LOGGER.warn(CONFIG_FILE_NAME + " is not exists");
            }
            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.error("ClusterController LoadMyidPropersites error:", e);
        }
        ClusterConfig clusterConfig = ClusterConfig.getInstance();

        ParameterMapping.mapping(clusterConfig, pros, null);
        if (pros.size() > 0) {
            String[] propItem = new String[pros.size()];
            pros.keySet().toArray(propItem);
            throw new ConfigException("These properties of system are not recognized: " + StringUtil.join(propItem, ","));
        }
        if (clusterConfig.isClusterEnable()) {
            if (Strings.isNullOrEmpty(clusterConfig.getClusterIP()) ||
                    Strings.isNullOrEmpty(clusterConfig.getClusterID())) {
                throw new RuntimeException("Cluster Config is not completely set");
            }
        }
    }

}
