/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

public final class DbleTempConfig {

    private static final DbleTempConfig INSTANCE = new DbleTempConfig();

    private DbleTempConfig() {
    }

    private String dbConfig;
    private String shardingConfig;
    private String userConfig;
    private String sequenceConfig;

    public String getDbConfig() {
        return dbConfig;
    }

    public String getShardingConfig() {
        return shardingConfig;
    }

    public String getUserConfig() {
        return userConfig;
    }

    public String getSequenceConfig() {
        return sequenceConfig;
    }

    public void setDbConfig(String dbConfig) {
        this.dbConfig = dbConfig;
    }

    public void setShardingConfig(String shardingConfig) {
        this.shardingConfig = shardingConfig;
    }

    public void setUserConfig(String userConfig) {
        this.userConfig = userConfig;
    }

    public void setSequenceConfig(String sequenceConfig) {
        this.sequenceConfig = sequenceConfig;
    }

    public static DbleTempConfig getInstance() {
        return INSTANCE;
    }

    public void clean() {
        this.dbConfig = null;
        this.shardingConfig = null;
        this.userConfig = null;
        this.sequenceConfig = null;
    }
}
