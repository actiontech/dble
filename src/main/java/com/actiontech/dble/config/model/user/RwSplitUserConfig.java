/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

public class RwSplitUserConfig extends ServerUserConfig {
    private final String dbGroup;

    public RwSplitUserConfig(UserConfig user, String tenant, WallProvider blacklist, String dbGroup) {
        super(user, tenant, blacklist);
        this.dbGroup = dbGroup;
    }

    public String getDbGroup() {
        return dbGroup;
    }


    public boolean equalsBaseInfo(RwSplitUserConfig rwSplitUserConfig) {
        return super.equalsBaseInfo(rwSplitUserConfig) &&
                StringUtil.equalsWithEmpty(this.dbGroup, rwSplitUserConfig.getDbGroup());
    }

}
