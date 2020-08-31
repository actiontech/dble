/*
* Copyright (C) 2016-2020 ActionTech.
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
    private volatile boolean skipIdleCheck = false;

    public ManagerConnection(NetworkChannel channel) throws IOException {
        super(channel);
    }

    @Override
    public boolean isIdleTimeout() {
        if (skipIdleCheck) {
            return false;
        } else if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
                    lastReadTime) + AUTH_TIMEOUT;
        }
    }

    @Override
    protected void setRequestTime() {
        //do nothing
    }

    @Override
    public void preparePushToQueue() {

    }

    @Override
    public void finishPushToQueue() {

    }

    @Override
    public void startProcess() {
        //do nothing
    }

    @Override
    public void markFinished() {
        //do nothing
    }

    @Override
    public void handle(final byte[] data) {
        handler.handle(data);
    }

    @Override
    public void killAndClose(String reason) {
        this.close(reason);
    }

    public void skipIdleCheck(boolean skip) {
        this.skipIdleCheck = skip;
    }

}
