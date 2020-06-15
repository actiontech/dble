/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import java.util.HashSet;
import java.util.Set;

public class ShardingUserConfig extends ServerUserConfig {
    private boolean readOnly;
    private Set<String> schemas;
    private UserPrivilegesConfig privilegesConfig;

    public ShardingUserConfig(String name, String password, String strWhiteIPs, String strMaxCon) {
        super(name, password, strWhiteIPs, strMaxCon);
    }


    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }



    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }



    public UserPrivilegesConfig getPrivilegesConfig() {
        return privilegesConfig;
    }

    public void setPrivilegesConfig(UserPrivilegesConfig privilegesConfig) {
        this.privilegesConfig = privilegesConfig;
    }
    public void changeMapToLowerCase() {
        Set<String> newSchemas = new HashSet<>();
        for (String schemaName : schemas) {
            newSchemas.add(schemaName.toLowerCase());
        }
        schemas = newSchemas;
    }

}
