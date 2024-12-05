/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.factory;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.backend.pool.PooledConnectionListener;
import com.oceanbase.obsharding_d.backend.pool.ReadTimeStatusInstance;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.SocketWR;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.PooledConnection;
import com.oceanbase.obsharding_d.net.impl.aio.AIOSocketWR;
import com.oceanbase.obsharding_d.net.impl.nio.NIOSocketWR;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLConnectionFactory extends PooledConnectionFactory {


    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, ResponseHandler handler, String schema) throws IOException {
        NetworkChannel channel;
        SocketWR socketWR;
        if (SystemConfig.getInstance().getUsingAIO() != 1) {
            channel = SocketChannel.open();
            ((SocketChannel) channel).configureBlocking(false);
            socketWR = new NIOSocketWR();
        } else {
            channel = AsynchronousSocketChannel.open(OBsharding_DServer.getInstance().getNextAsyncChannelGroup());
            socketWR = new AIOSocketWR();
        }
        BackendConnection connection = new BackendConnection(channel, socketWR, instance, handler, schema);
        connection.setSocketParams(false);
        socketWR.initFromConnection(connection);
        OBsharding_DServer.getInstance().getConnector().postConnect(connection);
        return connection;
    }

    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException {
        NetworkChannel channel;
        SocketWR socketWR;
        if (SystemConfig.getInstance().getUsingAIO() != 1) {
            channel = SocketChannel.open();
            ((SocketChannel) channel).configureBlocking(false);
            socketWR = new NIOSocketWR();
        } else {
            channel = AsynchronousSocketChannel.open(OBsharding_DServer.getInstance().getNextAsyncChannelGroup());
            socketWR = new AIOSocketWR();
        }

        BackendConnection connection = new BackendConnection(channel, socketWR, instance, listener, schema);
        socketWR.initFromConnection(connection);
        connection.setSocketParams(false);
        OBsharding_DServer.getInstance().getConnector().postConnect(connection);
        return connection;
    }
}
