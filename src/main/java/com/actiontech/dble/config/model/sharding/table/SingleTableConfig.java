/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.sharding.table;

import java.util.List;

public final class SingleTableConfig extends BaseTableConfig {

    public SingleTableConfig(String name, int maxLimit, List<String> shardingNodes, boolean specifyCharset) {
        super(name, maxLimit, shardingNodes, specifyCharset);
    }

    @Override
    public BaseTableConfig lowerCaseCopy(BaseTableConfig parent) {
        SingleTableConfig config = new SingleTableConfig(this.name.toLowerCase(), this.maxLimit, this.shardingNodes, this.specifyCharset);
        config.setId(getId());
        return config;
    }
}
