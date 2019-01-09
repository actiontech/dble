/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

/**
 * Created by szf on 2018/2/28.
 */
public enum ClusterParamCfg {
    /**
     * enable zk?ucore?
     */
    CLUSTER_FLAG("cluster"),

    /**
     * the url of ucore
     */
    CLUSTER_PLUGINS_IP("ipAddress"),

    /**
     * the port of ucore
     */
    CLUSTER_PLUGINS_PORT("port"),

    /**
     * clusterId
     */
    CLUSTER_CFG_CLUSTERID("clusterId"),

    /**
     * node id
     */
    CLUSTER_CFG_MYID("myid"),

    /**
     * serverID
     */
    CLUSTER_CFG_SERVER_ID("serverID"),;


    ClusterParamCfg(String key) {
        this.key = key;
    }

    private String key;

    public String getKey() {
        return key;
    }
}
