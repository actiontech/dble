/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.AuthPacket;

public class ManagerAuthenticator extends FrontendAuthenticator {
    public ManagerAuthenticator(FrontendConnection source) {
        super(source);
    }

    @Override
    protected void setConnProperties(AuthPacket auth) {
        ManagerConnection sc = (ManagerConnection) source;
        UserName user = new UserName(auth.getUser(), "");
        sc.setUser(user);
        sc.setAuthenticated(true);
        sc.setUserConfig((ManagerUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(user)));
        sc.initCharsetIndex(auth.getCharsetIndex());
        sc.setHandler(new ManagerCommandHandler(sc));
        sc.setClientFlags(auth.getClientFlags());
    }
}
