/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.factorys;


import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.factory.FrontendConnectionFactory;
import com.actiontech.dble.services.mysqlauthenticate.MySQLFrontAuthService;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ManagerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel, SocketWR socketWR) throws IOException {
        FrontendConnection c = new FrontendConnection(channel, socketWR, true);
        c.setService(new MySQLFrontAuthService(c));
        return c;
    }
}
