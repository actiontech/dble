/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.sharding.table;

import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;

import java.util.List;

public class ShardingTableConfig extends BaseTableConfig {
    private final String incrementColumn;
    private final AbstractPartitionAlgorithm function;
    private final String shardingColumn;
    private final boolean sqlRequiredSharding;

    public ShardingTableConfig(String name, int maxLimit, List<String> shardingNodes, String incrementColumn,
                                AbstractPartitionAlgorithm function, String shardingColumn, boolean sqlRequiredSharding) {
        super(name, maxLimit, shardingNodes);
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
        return new ShardingTableConfig(this.name.toLowerCase(), this.maxLimit, this.shardingNodes,
                this.incrementColumn, this.function, this.shardingColumn, this.sqlRequiredSharding);
    }
}
