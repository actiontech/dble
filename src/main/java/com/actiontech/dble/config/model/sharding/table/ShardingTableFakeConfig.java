package com.actiontech.dble.config.model.sharding.table;

import com.actiontech.dble.meta.table.MetaHelper;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;

import java.util.List;

public class ShardingTableFakeConfig extends ShardingTableConfig {

    public ShardingTableFakeConfig(String name, int maxLimit, List<String> shardingNodes, AbstractPartitionAlgorithm function, String createSql) {
        super(name, maxLimit, shardingNodes, null, function,
                MetaHelper.electionShardingColumn(createSql).toUpperCase(), false);
    }
}

