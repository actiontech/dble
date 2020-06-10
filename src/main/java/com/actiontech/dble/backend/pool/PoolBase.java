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

    private volatile boolean testOnCreate = false;
    private volatile boolean testOnBorrow = false;
    private volatile boolean testOnReturn = false;
    private volatile boolean testWhileIdle = false;
    private volatile long timeBetweenEvictionRunsMillis = -1L;
    private volatile int numTestsPerEvictionRun = 3;
    private volatile long evictorShutdownTimeoutMillis = 10000L;

    public PoolBase(DbInstanceConfig config, PhysicalDbInstance instance) {
        this.config = config;
        this.instance = instance;

        setConfig(config);
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
                public void onSuccess(BackendConnection conn) {
                    handler.connectionAcquired(conn);
                }

                @Override
                public void onError(BackendConnection conn, Throwable e) {
                    handler.connectionError(e, conn);
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
            listener.onError(null, ioe);
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

    /**
     * Initializes the receiver with the given configuration.
     *
     * @param config Initialization source.
     */
    protected void setConfig(DbInstanceConfig config) {
        setTestOnCreate(config.isTestOnCreate());
        setTestOnBorrow(config.isTestOnBorrow());
        setTestOnReturn(config.isTestOnReturn());
        setTestWhileIdle(config.isTestWhileIdle());
        setNumTestsPerEvictionRun(config.getNumTestsPerEvictionRun());
        setTimeBetweenEvictionRunsMillis(config.getTimeBetweenEvictionRunsMillis());
        setEvictorShutdownTimeoutMillis(config.getEvictorShutdownTimeoutMillis());
    }

    /**
     * Returns whether objects created for the pool will be validated before
     * being returned from the <code>borrowObject()</code> method. Validation is
     * performed by the <code>validateObject()</code> method of the factory
     * associated with the pool. If the object fails to validate, then
     * <code>borrowObject()</code> will fail.
     *
     * @return <code>true</code> if newly created objects are validated before
     * being returned from the <code>borrowObject()</code> method
     * @see #setTestOnCreate
     * @since 2.2
     */
    public final boolean getTestOnCreate() {
        return testOnCreate;
    }

    /**
     * Sets whether objects created for the pool will be validated before
     * being returned from the <code>borrowObject()</code> method. Validation is
     * performed by the <code>validateObject()</code> method of the factory
     * associated with the pool. If the object fails to validate, then
     * <code>borrowObject()</code> will fail.
     *
     * @param testOnCreate <code>true</code> if newly created objects should be
     *                     validated before being returned from the
     *                     <code>borrowObject()</code> method
     * @see #getTestOnCreate
     * @since 2.2
     */
    public final void setTestOnCreate(final boolean testOnCreate) {
        this.testOnCreate = testOnCreate;
    }

    /**
     * Returns whether objects borrowed from the pool will be validated before
     * being returned from the <code>borrowObject()</code> method. Validation is
     * performed by the <code>validateObject()</code> method of the factory
     * associated with the pool. If the object fails to validate, it will be
     * removed from the pool and destroyed, and a new attempt will be made to
     * borrow an object from the pool.
     *
     * @return <code>true</code> if objects are validated before being returned
     * from the <code>borrowObject()</code> method
     * @see #setTestOnBorrow
     */
    public final boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    /**
     * Sets whether objects borrowed from the pool will be validated before
     * being returned from the <code>borrowObject()</code> method. Validation is
     * performed by the <code>validateObject()</code> method of the factory
     * associated with the pool. If the object fails to validate, it will be
     * removed from the pool and destroyed, and a new attempt will be made to
     * borrow an object from the pool.
     *
     * @param testOnBorrow <code>true</code> if objects should be validated
     *                     before being returned from the
     *                     <code>borrowObject()</code> method
     * @see #getTestOnBorrow
     */
    public final void setTestOnBorrow(final boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    /**
     * Returns whether objects borrowed from the pool will be validated when
     * they are returned to the pool via the <code>returnObject()</code> method.
     * Validation is performed by the <code>validateObject()</code> method of
     * the factory associated with the pool. Returning objects that fail validation
     * are destroyed rather then being returned the pool.
     *
     * @return <code>true</code> if objects are validated on return to
     * the pool via the <code>returnObject()</code> method
     * @see #setTestOnReturn
     */
    public final boolean getTestOnReturn() {
        return testOnReturn;
    }

    /**
     * Sets whether objects borrowed from the pool will be validated when
     * they are returned to the pool via the <code>returnObject()</code> method.
     * Validation is performed by the <code>validateObject()</code> method of
     * the factory associated with the pool. Returning objects that fail validation
     * are destroyed rather then being returned the pool.
     *
     * @param testOnReturn <code>true</code> if objects are validated on
     *                     return to the pool via the
     *                     <code>returnObject()</code> method
     * @see #getTestOnReturn
     */
    public final void setTestOnReturn(final boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    /**
     * Returns whether objects sitting idle in the pool will be validated by the
     * idle object evictor (if any - see
     * {@link #setTimeBetweenEvictionRunsMillis(long)}). Validation is performed
     * by the <code>validateObject()</code> method of the factory associated
     * with the pool. If the object fails to validate, it will be removed from
     * the pool and destroyed.
     *
     * @return <code>true</code> if objects will be validated by the evictor
     * @see #setTestWhileIdle
     * @see #setTimeBetweenEvictionRunsMillis
     */
    public final boolean getTestWhileIdle() {
        return testWhileIdle;
    }

    /**
     * Returns whether objects sitting idle in the pool will be validated by the
     * idle object evictor (if any - see
     * {@link #setTimeBetweenEvictionRunsMillis(long)}). Validation is performed
     * by the <code>validateObject()</code> method of the factory associated
     * with the pool. If the object fails to validate, it will be removed from
     * the pool and destroyed.  Note that setting this property has no effect
     * unless the idle object evictor is enabled by setting
     * <code>timeBetweenEvictionRunsMillis</code> to a positive value.
     *
     * @param testWhileIdle <code>true</code> so objects will be validated by the evictor
     * @see #getTestWhileIdle
     * @see #setTimeBetweenEvictionRunsMillis
     */
    public final void setTestWhileIdle(final boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }

    /**
     * Returns the number of milliseconds to sleep between runs of the idle
     * object evictor thread. When non-positive, no idle object evictor thread
     * will be run.
     *
     * @return number of milliseconds to sleep between evictor runs
     * @see #setTimeBetweenEvictionRunsMillis
     */
    public final long getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    /**
     * Sets the number of milliseconds to sleep between runs of the idle object evictor thread.
     * <ul>
     * <li>When positive, the idle object evictor thread starts.</li>
     * <li>When non-positive, no idle object evictor thread runs.</li>
     * </ul>
     *
     * @param timeBetweenEvictionRunsMillis number of milliseconds to sleep between evictor runs
     * @see #getTimeBetweenEvictionRunsMillis
     */
    public final void setTimeBetweenEvictionRunsMillis(
            final long timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public final int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public final void setNumTestsPerEvictionRun(final int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
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

    /**
     * Sets the timeout that will be used when waiting for the Evictor to
     * shutdown if this pool is closed and it is the only pool still using the
     * the value for the Evictor.
     *
     * @param evictorShutdownTimeoutMillis the timeout in milliseconds that
     *                                     will be used while waiting for the
     *                                     Evictor to shut down.
     */
    public final void setEvictorShutdownTimeoutMillis(
            final long evictorShutdownTimeoutMillis) {
        this.evictorShutdownTimeoutMillis = evictorShutdownTimeoutMillis;
    }
}
