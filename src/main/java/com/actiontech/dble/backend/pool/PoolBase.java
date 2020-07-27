package com.actiontech.dble.backend.pool;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnectionAuthenticator;
import com.actiontech.dble.backend.mysql.nio.MySQLConnectionListener;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.db.DbInstanceConfig;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

public class PoolBase {

    protected final DbInstanceConfig config;
    protected final PhysicalDbInstance instance;

    public PoolBase(DbInstanceConfig dbConfig, PhysicalDbInstance instance) {
        this.config = dbConfig;
        this.instance = instance;
    }

    /**
     * only for heartbeat
     *
     * @param handler
     * @return
     */
    public void newConnection(String schema, ResponseHandler handler) {
        try {
            MySQLConnection conn = new MySQLConnection(openSocketChannel(), config, instance.isReadInstance(), instance.isAutocommitSynced(), instance.isIsolationSynced());
            conn.setSocketParams(false);
            conn.setSchema(schema);
            conn.setHandler(new MySQLConnectionAuthenticator(conn, new MySQLConnectionListener() {
                @Override
                public void onCreateSuccess(BackendConnection conn) {
                    handler.connectionAcquired(conn);
                }

                @Override
                public void onCreateFail(BackendConnection conn, Throwable e) {
                    handler.connectionError(e, null);
                }

                @Override
                public void onHeartbeatSuccess(BackendConnection conn) {
                }
            }));

            conn.connect();
        } catch (IOException ioe) {
            handler.connectionError(ioe, null);
        }
    }

    BackendConnection newConnection(String schema, MySQLConnectionListener listener) {
        MySQLConnection conn = null;
        try {
            conn = new MySQLConnection(openSocketChannel(), config, instance.isReadInstance(), instance.isAutocommitSynced(), instance.isIsolationSynced());
            conn.setSocketParams(false);
            conn.setSchema(schema);
            conn.setHandler(new MySQLConnectionAuthenticator(conn, listener));
            conn.connect();
            return conn;
        } catch (IOException ioe) {
            listener.onCreateFail(conn, ioe);
            return null;
        }
    }

    private NetworkChannel openSocketChannel() throws IOException {
        NetworkChannel channel;
        if (DbleServer.getInstance().isAIO()) {
            channel = AsynchronousSocketChannel.open(DbleServer.getInstance().getNextAsyncChannelGroup());
        } else {
            channel = SocketChannel.open();
            ((SocketChannel) channel).configureBlocking(false);
        }
        return channel;
    }

}
