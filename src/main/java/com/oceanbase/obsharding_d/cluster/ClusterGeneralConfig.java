/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.cluster.general.impl.UcoreSender;
import com.oceanbase.obsharding_d.cluster.general.impl.ushard.UshardSender;
import com.oceanbase.obsharding_d.cluster.zkprocess.ZkSender;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;

import static com.oceanbase.obsharding_d.cluster.ClusterController.*;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterGeneralConfig {

    private static final ClusterGeneralConfig INSTANCE = new ClusterGeneralConfig();
    private ClusterSender clusterSender = null;
    private String clusterType = null;
    private volatile boolean needBlocked = false;

    private ClusterGeneralConfig() {
    }

    public boolean isNeedBlocked() {
        return needBlocked;
    }

    public ClusterGeneralConfig setNeedBlocked(boolean needBlockedTmp) {
        needBlocked = needBlockedTmp;
        return this;
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
                    // must be extend from ClusterSender
                    if (!ClusterSender.class.isAssignableFrom(clz)) {
                        throw new IllegalArgumentException("No ClusterSender AS " + clazz);
                    }
                    INSTANCE.clusterSender = (ClusterSender) clz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("Get error when try to create " + clazz);
                }
        }
    }


    static void initData() throws Exception {
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
