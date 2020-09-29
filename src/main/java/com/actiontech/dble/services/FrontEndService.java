/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;

public abstract class FrontEndService extends VariablesService {
    public FrontEndService(AbstractConnection connection) {
        super(connection);
    }

    public abstract void userConnectionCount();

    public abstract UserName getUser();

    public abstract String getExecuteSql();

    public abstract void killAndClose(String reason);
}
