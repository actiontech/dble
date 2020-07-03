/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.factory;


import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.connection.FrontendConnection;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public abstract class FrontendConnectionFactory {
    protected abstract FrontendConnection getConnection(NetworkChannel channel, SocketWR socketWR)
            throws IOException;

    public FrontendConnection make(NetworkChannel channel, SocketWR socketWR) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        FrontendConnection c = getConnection(channel, socketWR);
        c.setSocketParams(true);
        return c;
    }


}
