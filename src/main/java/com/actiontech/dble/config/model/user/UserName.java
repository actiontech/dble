/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.util.StringUtil;

public class UserName {
    private final String name;
    private final String tenant;
    private int hashCode = -1;

    public UserName(String name, String tenant) {
        this.name = name;
        if (StringUtil.isEmpty(tenant)) {
            this.tenant = null;
        } else {
            this.tenant = tenant;
        }
    }

    public UserName(String name) {
        this.name = name;
        this.tenant = null;
    }

    @Override
    public String toString() {
        if (tenant == null) {
            return name;
        }
        return name + ":" + tenant;
    }

    private static final int HASH_CONST = 37;

    @Override
    public int hashCode() {
        if (hashCode == -1) {
            int hash = 17;
            hash = hash << 5 + hash << 1 + hash + name.hashCode();
            if (tenant == null) {
                hash += HASH_CONST;
            } else {
                hash = hash << 5 + hash << 1 + hash + tenant.hashCode();
            }
            hashCode = hash;
        }
        return hashCode;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UserName)) {
            return false;
        }
        UserName that = (UserName) obj;
        return isEquals(this.name, that.name) && isEquals(this.tenant, that.tenant);
    }

    private boolean isEquals(Object o1, Object o2) {
        if (o1 == null) {
            return o2 == null;
        }
        return o1 == o2 || o1.equals(o2);
    }

    public String getTenant() {
        return tenant;
    }

    public String getName() {
        return name;
    }
}
