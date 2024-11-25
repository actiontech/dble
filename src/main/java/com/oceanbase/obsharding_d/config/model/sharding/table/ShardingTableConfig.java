/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.sharding.table;

import com.oceanbase.obsharding_d.route.function.AbstractPartitionAlgorithm;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.util.List;

public class ShardingTableConfig extends BaseTableConfig {
    private final String incrementColumn;
    private final AbstractPartitionAlgorithm function;
    private final String shardingColumn;
    private final boolean sqlRequiredSharding;

    public ShardingTableConfig(String name, int maxLimit, List<String> shardingNodes, String incrementColumn,
                               AbstractPartitionAlgorithm function, String shardingColumn, boolean sqlRequiredSharding, boolean specifyCharset) {
        super(name, maxLimit, shardingNodes, specifyCharset);
        this.incrementColumn = incrementColumn;
        this.function = function;
        this.shardingColumn = shardingColumn;
        this.sqlRequiredSharding = sqlRequiredSharding;
    }

    public String getIncrementColumn() {
        return incrementColumn;
    }

    public AbstractPartitionAlgorithm getFunction() {
        return function;
    }


    public String getShardingColumn() {
        return shardingColumn;
    }

    public boolean isSqlRequiredSharding() {
        return sqlRequiredSharding;
    }

    @Override
    public BaseTableConfig lowerCaseCopy(BaseTableConfig parent) {
        ShardingTableConfig config = new ShardingTableConfig(this.name.toLowerCase(), this.maxLimit, this.shardingNodes,
                this.incrementColumn, this.function, this.shardingColumn, this.sqlRequiredSharding, this.specifyCharset);
        config.setId(this.getId());
        return config;
    }


    public boolean equalsBaseInfo(ShardingTableConfig shardingTableConfig) {
        return super.equalsBaseInfo(shardingTableConfig) &&
                StringUtil.equalsWithEmpty(this.incrementColumn, shardingTableConfig.getIncrementColumn()) &&
                this.function.equals(shardingTableConfig.getFunction()) &&
                StringUtil.equalsWithEmpty(this.shardingColumn, shardingTableConfig.getShardingColumn()) &&
                this.sqlRequiredSharding == shardingTableConfig.isSqlRequiredSharding();
    }
}
