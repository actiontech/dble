/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

public class ManagerUserConfig extends UserConfig {
    private final boolean readOnly;

    public ManagerUserConfig(UserConfig user, boolean readOnly) {
        super(user);
        if (whiteIPs.size() > 0) {
            whiteIPs.add("127.0.0.1");
            whiteIPs.add("0:0:0:0:0:0:0:1");
        }
        this.readOnly = readOnly;
    }


    public boolean isReadOnly() {
        return readOnly;
    }

}
