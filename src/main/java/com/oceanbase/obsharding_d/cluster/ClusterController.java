/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.ZktoXmlMain;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.util.ParameterMapping;
import com.oceanbase.obsharding_d.config.util.StartProblemReporter;
import com.oceanbase.obsharding_d.util.ResourceUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
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

    private ClusterController() {
    }

    public static void init() {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            //read from cluster.cnf to tall use zk or ucore
            try {
                ClusterGeneralConfig.initConfig();
                ClusterGeneralConfig.initData();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOGGER.info("No Cluster Config .......start in single mode");
        }
    }


    public static void loadClusterProperties() throws InvocationTargetException, IllegalAccessException {
        Properties pros = readClusterProperties();
        ClusterConfig clusterConfig = ClusterConfig.getInstance();

        ParameterMapping.mapping(clusterConfig, pros, StartProblemReporter.getInstance());
        if (pros.size() > 0) {
            String[] propItem = new String[pros.size()];
            pros.keySet().toArray(propItem);
            StartProblemReporter.getInstance().addError("These properties in cluster.cnf are not recognized: " + StringUtil.join(propItem, ","));
        }
        if (clusterConfig.isClusterEnable()) {
            if (Strings.isNullOrEmpty(clusterConfig.getClusterIP())) {
                StartProblemReporter.getInstance().addError("clusterIP need to set in cluster.cnf when clusterEnable is true");
            }
            if (Strings.isNullOrEmpty(clusterConfig.getClusterId())) {
                StartProblemReporter.getInstance().addError("clusterId need to set in cluster.cnf when clusterEnable is true");
            }
            if (Strings.isNullOrEmpty(clusterConfig.getRootPath())) {
                StartProblemReporter.getInstance().addError("rootPath need to set in cluster.cnf when clusterEnable is true");
            }
            int grpcTimeout = clusterConfig.getGrpcTimeout();
            if (grpcTimeout < 1) {
                StartProblemReporter.getInstance().addError("grpcTimeout should be greater than 1");
            }
        }
    }

    public static Properties readClusterProperties() {
        Properties pros = new Properties();

        try (InputStream configIS = ResourceUtil.getResourceAsStream(CONFIG_FILE_NAME)) {
            if (configIS == null) {
                LOGGER.warn(CONFIG_FILE_NAME + " is not exists");
            }
            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.error("ClusterController load " + CONFIG_FILE_NAME + " error:", e);
        }
        return pros;
    }

    public static boolean tryServerStartDuringInitClusterData() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            switch (ClusterConfig.getInstance().getClusterMode()) {
                // now only init zk data start OBsharding-D
                case ClusterController.CONFIG_MODE_ZK:
                    return ZktoXmlMain.serverStartDuringInitZKData();
                default:
                    return false;
            }
        }
        return false;
    }
}
