/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ServerUserConfig that = (ServerUserConfig) o;
        return Objects.equals(tenant, that.tenant) &&
                isEquals(this.blacklist, that.blacklist);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tenant, blacklist);
    }

    public boolean equalsBaseInfo(ServerUserConfig serverUserConfig) {
        return super.equalsBaseInfo(serverUserConfig) &&
                StringUtil.equalsWithEmpty(this.tenant, serverUserConfig.getTenant()) &&
                isEquals(this.blacklist, serverUserConfig.getBlacklist());
    }


    private boolean isEquals(WallProvider o1, WallProvider o2) {
        if (o1 == null || o2 == null) {
            return o1 == o2;
        }
        if (o1.getAttributes() == null || o2.getAttributes() == null) {
            return o1.getAttributes() == o2.getAttributes();
        }
        return o1 == o2 || o1.getAttributes().equals(o2.getAttributes());
    }

}
