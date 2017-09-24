/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.factory;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.FrontendConnection;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public abstract class FrontendConnectionFactory {
    protected abstract FrontendConnection getConnection(NetworkChannel channel)
            throws IOException;

    public FrontendConnection make(NetworkChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        FrontendConnection c = getConnection(channel);
        DbleServer.getInstance().getConfig().setSocketParams(c, true);
        return c;
    }


}
