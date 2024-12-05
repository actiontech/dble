/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.optimizer;

import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.SocketWR;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;

import java.io.IOException;
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

}
