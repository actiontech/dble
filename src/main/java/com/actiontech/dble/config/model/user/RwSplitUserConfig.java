/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

public class RwSplitUserConfig extends ServerUserConfig {
    private String dbGroup;

    public RwSplitUserConfig(String name, String password, String strWhiteIPs, int maxCon) {
        super(name, password, strWhiteIPs, maxCon);
    }

    public String getDbGroup() {
        return dbGroup;
    }

    public void setDbGroup(String dbGroup) {
        this.dbGroup = dbGroup;
    }


}
