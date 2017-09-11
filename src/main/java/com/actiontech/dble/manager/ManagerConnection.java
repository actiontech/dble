/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager;

import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.util.TimeUtil;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public class ManagerConnection extends FrontendConnection {
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    public ManagerConnection(NetworkChannel channel) throws IOException {
        super(channel);
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
                    lastReadTime) + AUTH_TIMEOUT;
        }
    }

    @Override
    public void handle(final byte[] data) {
        handler.handle(data);
    }

}
