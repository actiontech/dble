package com.actiontech.dble.backend.pool;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnectionAuthenticator;
import com.actiontech.dble.backend.mysql.nio.MySQLConnectionListener;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DbInstanceConfig;
import com.actiontech.dble.net.NIOConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

public class PoolBase {

    protected final DbInstanceConfig config;
    protected final PhysicalDbInstance instance;

    private final long connectionTimeout;
    private final long connectionHeartbeatTimeout;
    private final boolean testOnCreate;
    private final boolean testOnBorrow;
    private final boolean testOnReturn;
    private final boolean testWhileIdle;
    private final long timeBetweenEvictionRunsMillis;
    private final int numTestsPerEvictionRun;
    private final long evictorShutdownTimeoutMillis;
    private final long idleTimeout;

    public PoolBase(DbInstanceConfig dbConfig, PhysicalDbInstance instance) {
        this.config = dbConfig;
        this.instance = instance;

        PoolConfig poolConfig = dbConfig.getPoolConfig();
        this.testOnBorrow = poolConfig.getTestOnBorrow();
        this.testOnCreate = poolConfig.getTestOnCreate();
        this.testOnReturn = poolConfig.getTestOnReturn();
        this.testWhileIdle = poolConfig.getTestWhileIdle();
        this.connectionHeartbeatTimeout = poolConfig.getConnectionHeartbeatTimeout();
        this.connectionTimeout = poolConfig.getConnectionTimeout();
        this.timeBetweenEvictionRunsMillis = poolConfig.getTimeBetweenEvictionRunsMillis();
        this.numTestsPerEvictionRun = poolConfig.getNumTestsPerEvictionRun();
        this.evictorShutdownTimeoutMillis = poolConfig.getEvictorShutdownTimeoutMillis();
        this.idleTimeout = poolConfig.getIdleTimeout();
    }

    /**
     * only for heartbeat
     *
     * @param handler
     * @return
     */
    public void newConnection(String schema, ResponseHandler handler) {
        try {
            NetworkChannel channel = openSocketChannel();
            MySQLConnection conn = new MySQLConnection(channel, config, instance.isReadInstance(), instance.isAutocommitSynced(), instance.isIsolationSynced());
            conn.setSocketParams(false);
            conn.setSchema(schema);
            conn.setHandler(new MySQLConnectionAuthenticator(conn, new MySQLConnectionListener() {
                @Override
                public void onCreateSuccess(BackendConnection conn) {
                    handler.connectionAcquired(conn);
                }

                @Override
                public void onCreateFail(BackendConnection conn, Throwable e) {
                    handler.connectionError(e, conn);
                }

                @Override
                public void onHeartbeatSuccess(BackendConnection conn) {
                }

            }));

            if (channel instanceof AsynchronousSocketChannel) {
                ((AsynchronousSocketChannel) channel).connect(
                        new InetSocketAddress(config.getIp(), config.getPort()), conn,
                        (CompletionHandler) DbleServer.getInstance().getConnector());
            } else {
                ((NIOConnector) DbleServer.getInstance().getConnector()).postConnect(conn);
            }
        } catch (IOException ioe) {
            handler.connectionError(ioe, null);
        }
    }

    BackendConnection newConnection(String schema, MySQLConnectionListener listener) {
        try {
            NetworkChannel channel = openSocketChannel();

            MySQLConnection conn = new MySQLConnection(channel, config, instance.isReadInstance(), instance.isAutocommitSynced(), instance.isIsolationSynced());
            conn.setSocketParams(false);
            conn.setSchema(schema);
            conn.setHandler(new MySQLConnectionAuthenticator(conn, listener));
            conn.setDbInstance(instance);

            if (channel instanceof AsynchronousSocketChannel) {
                ((AsynchronousSocketChannel) channel).connect(
                        new InetSocketAddress(config.getIp(), config.getPort()), conn,
                        (CompletionHandler) DbleServer.getInstance().getConnector());
            } else {
                ((NIOConnector) DbleServer.getInstance().getConnector()).postConnect(conn);
            }
            return conn;

        } catch (IOException ioe) {
            listener.onCreateFail(null, ioe);
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

    public final boolean getTestOnCreate() {
        return testOnCreate;
    }

    public final boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    public final boolean getTestOnReturn() {
        return testOnReturn;
    }

    public final boolean getTestWhileIdle() {
        return testWhileIdle;
    }

    public final long getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public final int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    /**
     * Gets the timeout that will be used when waiting for the Evictor to
     * shutdown if this pool is closed and it is the only pool still using the
     * the value for the Evictor.
     *
     * @return The timeout in milliseconds that will be used while waiting for
     * the Evictor to shut down.
     */
    public final long getEvictorShutdownTimeoutMillis() {
        return evictorShutdownTimeoutMillis;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public long getConnectionHeartbeatTimeout() {
        return connectionHeartbeatTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }
}
