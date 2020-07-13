/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

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

}
