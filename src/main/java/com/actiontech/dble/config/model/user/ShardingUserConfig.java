/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.alibaba.druid.wall.WallProvider;

import java.util.HashSet;
import java.util.Set;

public class ShardingUserConfig extends ServerUserConfig {
    private final boolean readOnly;
    private Set<String> schemas;
    private final UserPrivilegesConfig privilegesConfig;

    public ShardingUserConfig(UserConfig user, String tenant, WallProvider blacklist, boolean readOnly, Set<String> schemas, UserPrivilegesConfig privilegesConfig) {
        super(user, tenant, blacklist);
        this.readOnly = readOnly;
        this.schemas = schemas;
        this.privilegesConfig = privilegesConfig;
    }

    public boolean isReadOnly() {
        return readOnly;
    }


    public Set<String> getSchemas() {
        return schemas;
    }


    public UserPrivilegesConfig getPrivilegesConfig() {
        return privilegesConfig;
    }

    public void changeMapToLowerCase() {
        Set<String> newSchemas = new HashSet<>();
        for (String schemaName : schemas) {
            newSchemas.add(schemaName.toLowerCase());
        }
        schemas = newSchemas;
    }

}
