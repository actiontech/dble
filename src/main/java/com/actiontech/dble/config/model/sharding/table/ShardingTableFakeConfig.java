/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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

