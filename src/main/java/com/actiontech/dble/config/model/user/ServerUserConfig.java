/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.alibaba.druid.wall.WallProvider;

public abstract class ServerUserConfig extends UserConfig {
    private String tenant;
    private WallProvider blacklist;

    public ServerUserConfig(String name, String password, String strWhiteIPs, String strMaxCon) {
        super(name, password, strWhiteIPs, strMaxCon);
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public WallProvider getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(WallProvider blacklist) {
        this.blacklist = blacklist;
    }
}
