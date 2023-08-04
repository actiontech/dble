/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.cluster.values.RawJson;

public final class DbleTempConfig {

    private static final DbleTempConfig INSTANCE = new DbleTempConfig();

    private DbleTempConfig() {
    }

    private RawJson dbConfig;
    private RawJson shardingConfig;
    private RawJson userConfig;
    private RawJson sequenceConfig;

    public RawJson getDbConfig() {
        return dbConfig;
    }

    public RawJson getShardingConfig() {
        return shardingConfig;
    }

    public RawJson getUserConfig() {
        return userConfig;
    }

    public RawJson getSequenceConfig() {
        return sequenceConfig;
    }

    public void setDbConfig(RawJson dbConfig) {
        this.dbConfig = dbConfig;
    }

    public void setShardingConfig(RawJson shardingConfig) {
        this.shardingConfig = shardingConfig;
    }

    public void setUserConfig(RawJson userConfig) {
        this.userConfig = userConfig;
    }

    public void setSequenceConfig(RawJson sequenceConfig) {
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
