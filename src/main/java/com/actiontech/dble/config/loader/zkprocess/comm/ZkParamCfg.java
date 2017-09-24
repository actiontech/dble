/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.comm;

/**
 * author:liujun
 * Created:2016/9/17
 */
public enum ZkParamCfg {

    /**
     * enable zk?
     */
    ZK_CFG_FLAG("loadZk"),

    /**
     * the url of zk
     */
    ZK_CFG_URL("zkURL"),

    /**
     * clusterId
     */
    ZK_CFG_CLUSTERID("clusterId"),

    /**
     * node id
     */
    ZK_CFG_MYID("myid"),;

    ZkParamCfg(String key) {
        this.key = key;
    }

    private String key;

    public String getKey() {
        return key;
    }


}
