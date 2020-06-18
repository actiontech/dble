/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.sharding.table;

import java.util.List;

public final class SingleTableConfig extends BaseTableConfig {

    public SingleTableConfig(String name, int maxLimit, List<String> shardingNodes) {
        super(name, maxLimit, shardingNodes);
    }

    @Override
    public BaseTableConfig lowerCaseCopy(BaseTableConfig parent) {
        return new SingleTableConfig(this.name.toLowerCase(), this.maxLimit, this.shardingNodes);
    }
}
