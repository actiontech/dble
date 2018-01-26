package com.actiontech.dble.config.loader.ucoreprocess;

/**
 * Created by szf on 2018/1/24.
 */
public enum UcoreParamCfg {

    /**
     * enable ucore?
     */
    UCORE_FLAGE("loadUcore"),

    /**
     * the url of ucore
     */
    UCORE_CFG_URL("uocreUrl"),

    /**
     * the port of ucore
     */
    UCORE_CFG_PORT("uocrePort"),

    /**
     * clusterId
     */
    UCORE_CFG_CLUSTERID("clusterId"),

    /**
     * node id
     */
    UCORE_CFG_MYID("myid"),;

    UcoreParamCfg(String key) {
        this.key = key;
    }

    private String key;

    public String getKey() {
        return key;
    }

}
