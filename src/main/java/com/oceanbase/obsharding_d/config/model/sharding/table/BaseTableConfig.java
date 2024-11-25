/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.sharding.table;

import com.oceanbase.obsharding_d.util.StringUtil;

import java.util.List;

public abstract class BaseTableConfig {

    private int id;
    protected final String name;
    protected final int maxLimit;
    protected final List<String> shardingNodes;
    protected boolean specifyCharset;

    BaseTableConfig(String name, int maxLimit, List<String> shardingNodes, boolean specifyCharset) {
        this.name = name;
        this.maxLimit = maxLimit;
        this.shardingNodes = shardingNodes;
        this.specifyCharset = specifyCharset;
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

    public boolean isSpecifyCharset() {
        return specifyCharset;
    }

    public abstract BaseTableConfig lowerCaseCopy(BaseTableConfig parent);


    public boolean equalsBaseInfo(BaseTableConfig baseTableConfig) {
        return StringUtil.equalsWithEmpty(this.name, baseTableConfig.getName()) &&
                this.maxLimit == baseTableConfig.getMaxLimit() &&
                isEquals(this.shardingNodes, baseTableConfig.getShardingNodes());
    }

    private boolean isEquals(List<String> o1, List<String> o2) {
        if (o1 == null) {
            return o2 == null;
        }
        return o1 == o2 || o1.equals(o2);
    }
}
