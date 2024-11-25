/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.sharding.table;

import com.oceanbase.obsharding_d.meta.table.MetaHelper;
import com.oceanbase.obsharding_d.route.function.AbstractPartitionAlgorithm;

import java.util.List;

public class ShardingTableFakeConfig extends ShardingTableConfig {

    public ShardingTableFakeConfig(String name, int maxLimit, List<String> shardingNodes, AbstractPartitionAlgorithm function, String createSql) {
        super(name, maxLimit, shardingNodes, null, function,
                MetaHelper.electionShardingColumn(createSql).toUpperCase(), false, false);
    }
}

