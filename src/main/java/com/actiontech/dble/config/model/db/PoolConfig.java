package com.actiontech.dble.config.model.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.*;

public class PoolConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolConfig.class);
    private static final String WARNING_FORMAT = "Property [ %s ] '%d' in db.xml is illegal, use the default value %d replaced";

    public static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
    public static final long CON_HEARTBEAT_TIMEOUT = MILLISECONDS.toMillis(20);
    public static final long DEFAULT_IDLE_TIMEOUT = MINUTES.toMillis(10);
    public static final long HOUSEKEEPING_PERIOD_MS = SECONDS.toMillis(30);
    public static final long DEFAULT_HEARTBEAT_PERIOD = SECONDS.toMillis(10);
    public static final long DEFAULT_SHUTDOWN_TIMEOUT = SECONDS.toMillis(10);

    private volatile long connectionTimeout = CONNECTION_TIMEOUT;
    private volatile long connectionHeartbeatTimeout = CON_HEARTBEAT_TIMEOUT;
    private volatile boolean testOnCreate = false;
    private volatile boolean testOnBorrow = false;
    private volatile boolean testOnReturn = false;
    private volatile boolean testWhileIdle = false;
    private volatile long timeBetweenEvictionRunsMillis = HOUSEKEEPING_PERIOD_MS;
    private volatile int numTestsPerEvictionRun = 3;
    private volatile long evictorShutdownTimeoutMillis = DEFAULT_SHUTDOWN_TIMEOUT;
    private volatile long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private volatile long heartbeatPeriodMillis = DEFAULT_HEARTBEAT_PERIOD;

    public PoolConfig() {
    }

    public PoolConfig(long connectionTimeout, long connectionHeartbeatTimeout, boolean testOnCreate, boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle, long timeBetweenEvictionRunsMillis, long evictorShutdownTimeoutMillis, long idleTimeout, long heartbeatPeriodMillis) {
        this.connectionTimeout = connectionTimeout;
        this.connectionHeartbeatTimeout = connectionHeartbeatTimeout;
        this.testOnCreate = testOnCreate;
        this.testOnBorrow = testOnBorrow;
        this.testOnReturn = testOnReturn;
        this.testWhileIdle = testWhileIdle;
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        this.evictorShutdownTimeoutMillis = evictorShutdownTimeoutMillis;
        this.idleTimeout = idleTimeout;
        this.heartbeatPeriodMillis = heartbeatPeriodMillis;
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
    public final void setTimeBetweenEvictionRunsMillis(final long timeBetweenEvictionRunsMillis) {
        if (timeBetweenEvictionRunsMillis <= 0) {
            LOGGER.warn(String.format(WARNING_FORMAT, "timeBetweenEvictionRunsMillis", timeBetweenEvictionRunsMillis, this.timeBetweenEvictionRunsMillis));
        } else {
            this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        }
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
    public final void setEvictorShutdownTimeoutMillis(final long evictorShutdownTimeoutMillis) {
        if (evictorShutdownTimeoutMillis <= 0) {
            LOGGER.warn(String.format(WARNING_FORMAT, "evictorShutdownTimeoutMillis", evictorShutdownTimeoutMillis, this.evictorShutdownTimeoutMillis));
        } else {
            this.evictorShutdownTimeoutMillis = evictorShutdownTimeoutMillis;
        }
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        if (connectionTimeout <= 0) {
            LOGGER.warn(String.format(WARNING_FORMAT, "connectionTimeout", connectionTimeout, this.connectionTimeout));
        } else {
            this.connectionTimeout = connectionTimeout;
        }
    }

    public long getConnectionHeartbeatTimeout() {
        return connectionHeartbeatTimeout;
    }

    public void setConnectionHeartbeatTimeout(long connectionHeartbeatTimeout) {
        if (connectionHeartbeatTimeout <= 0) {
            LOGGER.warn(String.format(WARNING_FORMAT, "connectionHeartbeatTimeout", connectionHeartbeatTimeout, this.connectionHeartbeatTimeout));
        } else {
            this.connectionHeartbeatTimeout = connectionHeartbeatTimeout;
        }
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        if (idleTimeout < 0) {
            LOGGER.warn(String.format(WARNING_FORMAT, "idleTimeout", idleTimeout, this.idleTimeout));
        } else {
            this.idleTimeout = idleTimeout;
        }
    }

    public long getHeartbeatPeriodMillis() {
        return heartbeatPeriodMillis;
    }

    public void setHeartbeatPeriodMillis(long heartbeatPeriodMillis) {
        if (heartbeatPeriodMillis <= 0) {
            LOGGER.warn(String.format(WARNING_FORMAT, "heartbeatPeriodMillis", heartbeatPeriodMillis, this.heartbeatPeriodMillis));
        } else {
            this.heartbeatPeriodMillis = heartbeatPeriodMillis;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PoolConfig that = (PoolConfig) o;

        if (connectionTimeout != that.connectionTimeout) return false;
        if (connectionHeartbeatTimeout != that.connectionHeartbeatTimeout) return false;
        if (testOnCreate != that.testOnCreate) return false;
        if (testOnBorrow != that.testOnBorrow) return false;
        if (testOnReturn != that.testOnReturn) return false;
        if (testWhileIdle != that.testWhileIdle) return false;
        if (timeBetweenEvictionRunsMillis != that.timeBetweenEvictionRunsMillis) return false;
        if (numTestsPerEvictionRun != that.numTestsPerEvictionRun) return false;
        if (evictorShutdownTimeoutMillis != that.evictorShutdownTimeoutMillis) return false;
        if (idleTimeout != that.idleTimeout) return false;
        return heartbeatPeriodMillis == that.heartbeatPeriodMillis;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
