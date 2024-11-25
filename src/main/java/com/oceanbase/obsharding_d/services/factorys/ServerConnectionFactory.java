/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.factorys;


import com.oceanbase.obsharding_d.net.SocketWR;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.factory.FrontendConnectionFactory;
import com.oceanbase.obsharding_d.services.mysqlauthenticate.MySQLFrontAuthService;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel, SocketWR socketWR) throws IOException {
        FrontendConnection c = new FrontendConnection(channel, socketWR, false);
        c.setService(new MySQLFrontAuthService(c));
        return c;
    }

}
