/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.general.AbstractClusterSender;
import com.actiontech.dble.cluster.general.impl.UcoreSender;
import com.actiontech.dble.cluster.general.impl.ushard.UshardSender;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.cluster.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.singleton.OnlineStatus;

import java.io.IOException;

import static com.actiontech.dble.backend.mysql.nio.handler.ResetConnHandler.LOGGER;
import static com.actiontech.dble.cluster.ClusterController.*;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterGeneralConfig {

    private static final ClusterGeneralConfig INSTANCE = new ClusterGeneralConfig();
    private AbstractClusterSender clusterSender = null;
    private String clusterType = null;

    private ClusterGeneralConfig() {

    }

    public static ClusterGeneralConfig initConfig() {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            switch (ClusterConfig.getInstance().getClusterMode()) {
                case CONFIG_MODE_USHARD:
                    INSTANCE.clusterSender = new UshardSender();
                    INSTANCE.clusterType = CONFIG_MODE_USHARD;
                    break;
                case CONFIG_MODE_UCORE:
                    INSTANCE.clusterSender = new UcoreSender();
                    INSTANCE.clusterType = CONFIG_MODE_UCORE;
                    break;
                case CONFIG_MODE_ZK:
                    INSTANCE.clusterType = CONFIG_MODE_ZK;
                    break;
                default:
                    String clazz = ClusterConfig.getInstance().getClusterMode();
                    try {
                        INSTANCE.clusterType = CONFIG_MODE_CUSTOMIZATION;
                        Class<?> clz = Class.forName(clazz);
                        //all function must be extend from AbstractPartitionAlgorithm
                        if (!AbstractClusterSender.class.isAssignableFrom(clz)) {
                            throw new IllegalArgumentException("No ClusterSender AS " + clazz);
                        }
                        INSTANCE.clusterSender = (AbstractClusterSender) clz.newInstance();
                    } catch (Exception e) {
                        throw new RuntimeException("Get error when try to create " + clazz);
                    }
            }
        } else {
            INSTANCE.clusterType = "No Cluster";
            LOGGER.info("No Cluster Config .......start in single mode");
        }

        return INSTANCE;
    }


    public static void initData() throws IOException {
        if (!ClusterConfig.getInstance().isClusterEnable()) {
            return;
        }
        if (CONFIG_MODE_ZK.equals(ClusterConfig.getInstance().getClusterMode())) {
            ZkConfig.initZk();
        } else {
            LOGGER.info("===================Init online status in cluster==================");
            try {
                OnlineStatus.getInstance().mainThreadInitClusterOnline();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.warn("cluster can not connection ", e);
            }
            INSTANCE.clusterSender.initCluster();
            ClusterToXml.loadKVtoFile();
        }
    }


    public AbstractClusterSender getClusterSender() {
        return clusterSender;
    }


    public String getClusterType() {
        return clusterType;
    }

    public static ClusterGeneralConfig getInstance() {
        return INSTANCE;
    }


}
