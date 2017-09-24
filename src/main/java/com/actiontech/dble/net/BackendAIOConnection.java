/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import com.actiontech.dble.backend.BackendConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public abstract class BackendAIOConnection extends AbstractConnection implements
        BackendConnection {

    public BackendAIOConnection(NetworkChannel channel) {
        super(channel);
    }

    public void register() throws IOException {
        this.asynRead();
    }


    public void setHost(String host) {
        this.host = host;
    }


    public void setPort(int port) {
        this.port = port;
    }

    public abstract void onConnectFailed(Throwable e);

    public boolean finishConnect() throws IOException {
        localPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
        return true;
    }

    public void setProcessor(NIOProcessor processor) {
        super.setProcessor(processor);
        processor.addBackend(this);
    }

    @Override
    public String toString() {
        return "BackendConnection [id=" + id + ", host=" + host + ", port=" + port +
                ", localPort=" + localPort + "]";
    }

    public String compactInfo() {
        return "BackendConnection host=" + host + ", port=" + port;
    }
}
