/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.alibaba.druid.wall.WallProvider;

public class ServerUserConfig extends UserConfig {
    private String tenant;
    private int maxCon;
    private WallProvider blacklist;

    public ServerUserConfig(String name, String password, String strWhiteIPs) {
        super(name, password, strWhiteIPs);
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
    }

    public WallProvider getBlacklist() {
        return blacklist;
    }

    public void setBlacklist(WallProvider blacklist) {
        this.blacklist = blacklist;
    }
}
