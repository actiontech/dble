/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

public class ManagerUserConfig extends UserConfig {
    private boolean readOnly;

    public ManagerUserConfig(String name, String password, String strWhiteIPs) {
        super(name, password, strWhiteIPs);
        if (whiteIPs.size() > 0) {
            whiteIPs.add("127.0.0.1");
            whiteIPs.add("0:0:0:0:0:0:0:1");
        }
    }


    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

}
