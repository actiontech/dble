package com.actiontech.dble.net.factory;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.impl.aio.AIOSocketWR;
import com.actiontech.dble.net.impl.nio.NIOSocketWR;

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
            channel = AsynchronousSocketChannel.open(DbleServer.getInstance().getNextAsyncChannelGroup());
            socketWR = new AIOSocketWR();
        }
        BackendConnection connection = new BackendConnection(channel, socketWR, instance, handler);
        connection.setSocketParams(false);
        socketWR.initFromConnection(connection);
        DbleServer.getInstance().getConnector().postConnect(connection);
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
            channel = AsynchronousSocketChannel.open(DbleServer.getInstance().getNextAsyncChannelGroup());
            socketWR = new AIOSocketWR();
        }

        BackendConnection connection = new BackendConnection(channel, socketWR, instance, listener, schema);
        socketWR.initFromConnection(connection);
        connection.setSocketParams(false);
        DbleServer.getInstance().getConnector().postConnect(connection);
        return connection;
    }
}
