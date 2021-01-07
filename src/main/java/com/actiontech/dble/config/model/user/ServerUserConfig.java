/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

public abstract class ServerUserConfig extends UserConfig {
    private final String tenant;
    private final WallProvider blacklist;

    ServerUserConfig(UserConfig user, String tenant, WallProvider blacklist) {
        super(user);
        this.tenant = tenant;
        this.blacklist = blacklist;
    }

    public String getTenant() {
        return tenant;
    }

    public WallProvider getBlacklist() {
        return blacklist;
    }


    public boolean equalsBaseInfo(ServerUserConfig serverUserConfig) {
        return super.equalsBaseInfo(serverUserConfig) &&
                StringUtil.equalsWithEmpty(this.tenant, serverUserConfig.getTenant()) &&
                isEquals(this.blacklist, serverUserConfig.getBlacklist());
    }


    private boolean isEquals(WallProvider o1, WallProvider o2) {
        if (o1 == null) {
            return o2 == null;
        }
        return o1 == o2 || o1.getAttributes().equals(o2.getAttributes());
    }

}
