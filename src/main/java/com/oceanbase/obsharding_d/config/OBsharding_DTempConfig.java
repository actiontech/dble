/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config;

import com.oceanbase.obsharding_d.cluster.values.RawJson;

public final class OBsharding_DTempConfig {

    private static final OBsharding_DTempConfig INSTANCE = new OBsharding_DTempConfig();

    private OBsharding_DTempConfig() {
    }

    private RawJson dbConfig;
    private RawJson shardingConfig;
    private RawJson userConfig;
    private RawJson sequenceConfig;
    private Boolean lowerCase;

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

    public Boolean isLowerCase() {
        return lowerCase;
    }

    public void setLowerCase(Boolean lowerCase) {
        this.lowerCase = lowerCase;
    }

    public static OBsharding_DTempConfig getInstance() {
        return INSTANCE;
    }

    public void clean() {
        this.dbConfig = null;
        this.shardingConfig = null;
        this.userConfig = null;
        this.sequenceConfig = null;
    }
}
