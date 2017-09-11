/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.model.UserConfig;

/**
 * @author mycat
 */
public final class ManagerPrivileges extends ServerPrivileges {
    private static ManagerPrivileges instance = new ManagerPrivileges();

    public static ManagerPrivileges instance() {
        return instance;
    }

    private ManagerPrivileges() {
        super();
    }

    protected boolean checkManagerPrivilege(String user) {
        ServerConfig config = DbleServer.getInstance().getConfig();
        UserConfig rUser = config.getUsers().get(user);
        // Manager privilege must be assign explicitly
        return rUser != null && rUser.isManager();
    }
}
