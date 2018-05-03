package com.actiontech.dble.cluster;

import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.util.ResourceUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by szf on 2018/2/28.
 */
public final class ClusterController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterController.class);

    public static final String CONFIG_FILE_NAME = "/myid.properties";
    private static final String CONFIG_MODE_UCORE = "ucore";
    private static final String CONFIG_MODE_ZK = "zk";
    private static final String CONFIG_MODE_SINGLE = "false";

    public static final int GRPC_SUBTIMEOUT = 70;
    public static final int GENERAL_GRPC_TIMEOUT = 10;


    private static Properties properties = null;

    private ClusterController() {
    }

    public static void init() {
        //read from myid.properties to tall use zk or ucore
        try {
            properties = loadMyidPropersites();

            if (CONFIG_MODE_UCORE.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
                UcoreConfig.initUcore(properties);
            } else if (CONFIG_MODE_ZK.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
                ZkConfig.initZk(properties);
            } else {
                LOGGER.info("No Cluster Config .......start in single mode");
            }
        } catch (Exception e) {
            LOGGER.warn(AlarmCode.CORE_CLUSTER_WARN + "error:", e);
        }

    }

    public static void initFromShellUcore() {
        properties = loadMyidPropersites();
        UcoreConfig.initUcoreFromShell(properties);
    }

    public static void initFromShellZK() {
        properties = loadMyidPropersites();
        ZkConfig.setZkProperties(properties);
    }


    private static Properties loadMyidPropersites() {
        Properties pros = new Properties();

        try (InputStream configIS = ResourceUtil.getResourceAsStream(CONFIG_FILE_NAME)) {
            if (configIS == null) {
                return null;
            }
            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.error(AlarmCode.CORE_ERROR + "ClusterController LoadMyidPropersites error:", e);
        }

        //check if the
        if (!CONFIG_MODE_SINGLE.equalsIgnoreCase(pros.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            if (Strings.isNullOrEmpty(pros.getProperty(ClusterParamCfg.CLUSTER_PLUGINS_IP.getKey())) ||
                    Strings.isNullOrEmpty(pros.getProperty(ClusterParamCfg.CLUSTER_PLUGINS_PORT.getKey())) ||
                    Strings.isNullOrEmpty(pros.getProperty(ClusterParamCfg.CLUSTER_CFG_CLUSTERID.getKey())) ||
                    Strings.isNullOrEmpty(pros.getProperty(ClusterParamCfg.CLUSTER_CFG_MYID.getKey()))) {
                throw new RuntimeException("Cluster Config is not completely set");
            }
        }
        return pros;

    }

}
