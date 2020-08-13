/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.sharding.table;

import java.util.List;

public abstract class BaseTableConfig {

    private int id;
    protected final String name;
    protected final int maxLimit;
    protected final List<String> shardingNodes;

    BaseTableConfig(String name, int maxLimit, List<String> shardingNodes) {
        this.name = name;
        this.maxLimit = maxLimit;
        this.shardingNodes = shardingNodes;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public int getMaxLimit() {
        return maxLimit;
    }


    public List<String> getShardingNodes() {
        return shardingNodes;
    }

    public abstract BaseTableConfig lowerCaseCopy(BaseTableConfig parent);
}
