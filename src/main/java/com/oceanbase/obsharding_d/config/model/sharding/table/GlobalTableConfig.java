/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.sharding.table;

import com.oceanbase.obsharding_d.util.StringUtil;

import java.util.List;

public class GlobalTableConfig extends BaseTableConfig {

    private final boolean globalCheck;
    private final String cron;
    private final String checkClass;

    public GlobalTableConfig(String name, int maxLimit, List<String> shardingNodes, String cron, String checkClass, boolean globalCheck, boolean specifyCharset) {
        super(name, maxLimit, shardingNodes, specifyCharset);
        this.cron = cron;
        this.checkClass = checkClass;
        this.globalCheck = globalCheck;
    }


    public boolean isGlobalCheck() {
        return globalCheck;
    }

    public String getCron() {
        return cron;
    }

    public String getCheckClass() {
        return checkClass;
    }

    @Override
    public BaseTableConfig lowerCaseCopy(BaseTableConfig parent) {
        GlobalTableConfig config = new GlobalTableConfig(this.name.toLowerCase(), this.maxLimit, this.shardingNodes, this.cron, this.checkClass, this.globalCheck, this.specifyCharset);
        config.setId(this.getId());
        return config;
    }


    public boolean equalsBaseInfo(GlobalTableConfig globalTableConfig) {
        return super.equalsBaseInfo(globalTableConfig) &&
                this.globalCheck == globalTableConfig.isGlobalCheck() &&
                StringUtil.equalsWithEmpty(this.cron, globalTableConfig.getCron()) &&
                StringUtil.equalsWithEmpty(this.checkClass, globalTableConfig.getCheckClass());
    }
}
