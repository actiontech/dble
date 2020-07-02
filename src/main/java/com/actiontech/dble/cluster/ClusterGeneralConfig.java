/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.general.impl.UcoreSender;
import com.actiontech.dble.cluster.general.impl.ushard.UshardSender;
import com.actiontech.dble.cluster.zkprocess.ZkSender;
import com.actiontech.dble.config.model.ClusterConfig;

import java.io.IOException;

import static com.actiontech.dble.cluster.ClusterController.*;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterGeneralConfig {

    private static final ClusterGeneralConfig INSTANCE = new ClusterGeneralConfig();
    private ClusterSender clusterSender = null;
    private String clusterType = null;

    private ClusterGeneralConfig() {
    }

    public static void initConfig() {
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
                INSTANCE.clusterSender = new ZkSender();
                INSTANCE.clusterType = CONFIG_MODE_ZK;
                break;
            default:
                String clazz = ClusterConfig.getInstance().getClusterMode();
                try {
                    INSTANCE.clusterType = CONFIG_MODE_CUSTOMIZATION;
                    Class<?> clz = Class.forName(clazz);
                    //all function must be extend from AbstractPartitionAlgorithm
                    if (!ClusterSender.class.isAssignableFrom(clz)) {
                        throw new IllegalArgumentException("No ClusterSender AS " + clazz);
                    }
                    INSTANCE.clusterSender = (ClusterSender) clz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("Get error when try to create " + clazz);
                }
        }
    }


    static void initData() throws IOException {
        INSTANCE.clusterSender.initCluster();
    }


    public ClusterSender getClusterSender() {
        return clusterSender;
    }


    public String getClusterType() {
        return clusterType;
    }

    public static ClusterGeneralConfig getInstance() {
        return INSTANCE;
    }


}
