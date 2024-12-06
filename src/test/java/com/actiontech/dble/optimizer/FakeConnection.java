/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.optimizer;

import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.connection.AbstractConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;

/**
 * @author dcy
 * Create Date: 2021-12-01
 */
public class FakeConnection extends AbstractConnection {

    public FakeConnection(NetworkChannel channel, SocketWR socketWR) {
        super(channel, socketWR);
    }

    @Override
    public IOProcessor getProcessor() {
        try {
            return new IOProcessor("", null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void setProcessor(IOProcessor processor) {

    }

    @Override
    public void startFlowControl(int currentWritingSize) {

    }

    @Override
    public void stopFlowControl(int currentWritingSize) {

    }

    @Override
    public void businessClose(String reason) {

    }

    @Override
    protected void handleNonSSL(ByteBuffer dataBuffer) throws IOException {

    }
}
