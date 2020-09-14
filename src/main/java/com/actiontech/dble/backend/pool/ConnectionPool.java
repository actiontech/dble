package com.actiontech.dble.backend.pool;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.actiontech.dble.btrace.provider.ConnectionPoolProvider;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.factory.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.net.connection.PooledConnection.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;


public class ConnectionPool extends PoolBase implements PooledConnectionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);

    private final AtomicInteger waiters;
    private final CopyOnWriteArrayList<PooledConnection> allConnections;
    private final AtomicInteger totalConnections = new AtomicInteger();
    private final SynchronousQueue<PooledConnection> handoffQueue;
    // evictor
    private final WeakReference<ClassLoader> factoryClassLoader;
    private volatile Evictor evictor = null;

    private final AtomicBoolean isClosed = new AtomicBoolean(true);
    private final PoolConfig poolConfig;
    private final ReentrantReadWriteLock freshLock;

    public ConnectionPool(final DbInstanceConfig config, final ReadTimeStatusInstance instance, final PooledConnectionFactory factory) {
        super(config, instance, factory);

        // save the current TCCL (if any) to be used later by the evictor Thread
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            factoryClassLoader = null;
        } else {
            factoryClassLoader = new WeakReference<>(cl);
        }

        this.handoffQueue = new SynchronousQueue<>(true);
        this.waiters = new AtomicInteger();
        this.allConnections = new CopyOnWriteArrayList<>();
        this.poolConfig = config.getPoolConfig();
        this.freshLock = new ReentrantReadWriteLock();
    }

    public PooledConnection borrowDirectly(final String schema) {
        if (!freshLock.readLock().tryLock()) {
            LOGGER.warn("the current thread is blocked, because currently at freshing conn");
            freshLock.readLock().lock();
        }
        try {
            ConnectionPoolProvider.getConnGetFrenshLocekAfter();
            for (PooledConnection conn : allConnections) {
                if (conn.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    newPooledEntry(schema, waiters.get());
                    return conn;
                }
            }
            return null;
        } finally {
            freshLock.readLock().unlock();
        }
    }

    public PooledConnection borrow(final String schema, long timeout, final TimeUnit timeUnit) throws InterruptedException {
        if (!freshLock.readLock().tryLock()) {
            LOGGER.warn("the current thread is blocked, because currently at freshing conn");
            freshLock.readLock().lock();
        }
        try {
            final int waiting = waiters.incrementAndGet();
            ConnectionPoolProvider.getConnGetFrenshLocekAfter();
            for (PooledConnection conn : allConnections) {
                if (conn.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    // If we may have stolen another waiter's connection, request another bag add.
                    if (waiting > 1) {
                        newPooledEntry(schema, waiting - 1);
                    }
                    return conn;
                }
            }

            newPooledEntry(schema, waiting);

            timeout = timeUnit.toNanos(timeout);

            do {
                final long start = System.nanoTime();
                final PooledConnection bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
                if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    return bagEntry;
                }

                timeout -= (System.nanoTime() - start);
            } while (timeout > 10_000);

            return null;
        } finally {
            waiters.decrementAndGet();
            freshLock.readLock().unlock();
        }
    }

    private void newPooledEntry(final String schema, final int waiting) {
        if (instance.isDisabled() || isClosed.get()) {
            return;
        }

        if (waiting > 0) {
            if (totalConnections.incrementAndGet() <= config.getMaxCon()) {
                final PooledConnection conn = newConnection(schema, ConnectionPool.this);
                if (conn != null) {
                    return;
                }
            }

            totalConnections.decrementAndGet();

            // alert
            String maxConError = "the max active Connections size can not be max than maxCon for dbInstance[" + instance.getDbGroupConfig().getName() + "." + config.getInstanceName() + "]";
            LOGGER.warn(maxConError);
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", instance.getDbGroupConfig().getName() + "-" + config.getInstanceName());
            AlertUtil.alert(AlarmCode.REACH_MAX_CON, Alert.AlertLevel.WARN, maxConError, "dble", config.getId(), labels);
            ToResolveContainer.REACH_MAX_CON.add(instance.getDbGroupConfig().getName() + "-" + config.getInstanceName());
        }
    }


    public void release(final PooledConnection conn) {
        if (poolConfig.getTestOnReturn()) {
            conn.synchronousTest();
        }

        conn.lazySet(STATE_NOT_IN_USE);
        for (int i = 0; waiters.get() > 0; i++) {
            if (conn.getState() != STATE_NOT_IN_USE || handoffQueue.offer(conn)) {
                return;
            } else if ((i & 0xff) == 0xff) {
                parkNanos(MICROSECONDS.toNanos(10));
            } else {
                Thread.yield();
            }
        }
    }

    private void fillPool() {
        final int idleCount = getCount(STATE_NOT_IN_USE, STATE_HEARTBEAT);
        final int connectionsToAdd = Math.min(config.getMaxCon() - totalConnections.get(), config.getMinCon() - idleCount) -
                (totalConnections.get() - allConnections.size());
        if (LOGGER.isDebugEnabled() && connectionsToAdd > 0) {
            LOGGER.debug("need add {}", connectionsToAdd);
        }
        for (int i = 0; i < connectionsToAdd; i++) {
            // newPooledEntry(schemas[i % schemas.length]);
            newPooledEntry(null, 1);
        }
    }

    @Override
    public void onCreateSuccess(PooledConnection conn) {
        conn.setPoolRelated(this);
        allConnections.add(conn);
        if (poolConfig.getTestOnCreate()) {
            conn.synchronousTest();
        }

        conn.lazySet(STATE_NOT_IN_USE);
        // spin until a thread takes it or none are waiting
        while (waiters.get() > 0 && conn.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(conn)) {
            Thread.yield();
        }

        if (ToResolveContainer.CREATE_CONN_FAIL.contains(instance.getDbGroupConfig().getName() + "-" + config.getInstanceName())) {
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", instance.getDbGroupConfig().getName() + "-" + config.getInstanceName());
            AlertUtil.alertResolve(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "mysql", config.getId(), labels,
                    ToResolveContainer.CREATE_CONN_FAIL, instance.getDbGroupConfig().getName() + "-" + config.getInstanceName());
        }
    }

    @Override
    public void onCreateFail(PooledConnection conn, Throwable e) {
        LOGGER.warn("create connection fail " + e.getMessage());
        totalConnections.decrementAndGet();
        // conn can be null if newChannel crashed (eg SocketException("too many open files"))
        if (conn != null) {
            conn.businessClose("create fail");
        }
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", instance.getDbGroupConfig().getName() + "-" + config.getInstanceName());
        AlertUtil.alert(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "createNewConn Error" + e.getMessage(), "mysql", config.getId(), labels);
        ToResolveContainer.CREATE_CONN_FAIL.add(instance.getDbGroupConfig().getName() + "-" + config.getInstanceName());
    }

    @Override
    public void onHeartbeatSuccess(PooledConnection conn) {
        conn.lazySet(STATE_NOT_IN_USE);
    }

    public int getCount(final int... states) {
        int count = 0;
        int curState;
        for (PooledConnection conn : allConnections) {
            curState = conn.getState();
            for (int state : states) {
                if (curState == state) {
                    count++;
                    break;
                }
            }
        }
        return count;
    }

    public int getCount(String schema, final int... states) {
        int count = 0;
        int curState;
        for (final PooledConnection conn : allConnections) {
            if (!schema.equals(conn.getSchema())) {
                continue;
            }
            curState = conn.getState();
            for (int state : states) {
                if (curState == state) {
                    count++;
                    break;
                }
            }
        }
        return count;
    }

    public int size() {
        return allConnections.size();
    }

    public boolean isFromSlave() {
        return !config.isPrimary();
    }

    public void close(final PooledConnection conn) {
        if (remove(conn)) {
            final int tc = totalConnections.decrementAndGet();
            if (tc < 0) {
                LOGGER.warn("{} - Unexpected value of totalConnections={}", config.getInstanceName(), tc);
            }
        }
    }

    private boolean remove(final PooledConnection pooledConnection) {
        final boolean removed = allConnections.remove(pooledConnection);
        if (!removed) {
            LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", pooledConnection);
        }

        return removed;
    }


    public void softCloseAllConnections(final String closureReason) {
        while (totalConnections.get() > 0) {
            for (PooledConnection conn : allConnections) {
                if (conn.getState() == STATE_IN_USE) {
                    close(conn);
                    conn.setPoolDestroyedTime(System.currentTimeMillis());
                    IOProcessor.BACKENDS_OLD.add(conn);
                } else {
                    conn.close(closureReason);
                }
            }
        }
    }

    /**
     * Closes the keyed object pool. Once the pool is closed
     */
    public void forceCloseAllConnection(final String closureReason) {
        while (totalConnections.get() > 0) {
            for (PooledConnection conn : allConnections) {
                if (conn.getState() == STATE_IN_USE) {
                    ((BackendConnection) conn).closeWithFront(closureReason);
                } else {
                    conn.close(closureReason);
                }
            }
        }
    }

    /**
     * Closes the keyed object pool. Once the pool is closed
     */
    public void stop(final String closureReason) {
        stop(closureReason, false);
    }

    public void stop(final String closureReason, boolean closeFront) {
        freshLock.writeLock().lock();
        try {
            ConnectionPoolProvider.stopConnGetFrenshLocekAfter();
            if (isClosed.compareAndSet(false, true)) {
                stopEvictor();
                if (closeFront) {
                    forceCloseAllConnection(closureReason);
                } else {
                    softCloseAllConnections(closureReason);
                }
            }
        } finally {
            freshLock.writeLock().unlock();
        }
    }

    private void evict() {

        final ArrayList<PooledConnection> idleList = new ArrayList<>(allConnections.size());
        for (final PooledConnection entry : allConnections) {
            if (entry.getState() == STATE_NOT_IN_USE) {
                idleList.add(entry);
            }
        }

        int removable = idleList.size() - config.getMinCon();

        // Sort pool entries on lastAccessed
        idleList.sort(LAST_ACCESS_COMPARABLE);

        logPoolState("before cleanup ");
        for (PooledConnection conn : idleList) {
            if (removable > 0 && System.currentTimeMillis() - conn.getLastTime() > poolConfig.getIdleTimeout() &&
                    conn.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED)) {
                conn.close("connection has passed idleTimeout");
                removable--;
            } else if (poolConfig.getTestWhileIdle() && conn.compareAndSet(STATE_NOT_IN_USE, STATE_HEARTBEAT)) {
                ConnectionHeartBeatHandler heartBeatHandler = new ConnectionHeartBeatHandler((BackendConnection) conn, false, this);
                heartBeatHandler.ping(poolConfig.getConnectionHeartbeatTimeout());
                conn.asynchronousTest();
            }
        }

    }

    public ReadTimeStatusInstance getInstance() {
        return instance;
    }

    public final int getThreadsAwaitingConnection() {
        return waiters.get();
    }

    /**
     * <p>Starts the evictor with the given delay. If there is an evictor
     * running when this method is called, it is stopped and replaced with a
     * new evictor with the specified delay.</p>
     *
     * <p>This method needs to be final, since it is called from a constructor.
     * See POOL-195.</p>
     */
    public void startEvictor() {
        if (isClosed.compareAndSet(true, false)) {
            if (evictor != null) {
                EvictionTimer.cancel(evictor, poolConfig.getEvictorShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
            }
            evictor = new Evictor();
            EvictionTimer.schedule(evictor, 0, poolConfig.getTimeBetweenEvictionRunsMillis());
        }
    }

    /**
     * Stops the evictor.
     */
    public void stopEvictor() {
        EvictionTimer.cancel(evictor, poolConfig.getEvictorShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        evictor = null;
    }

    /**
     * Log the current pool state at debug level.
     *
     * @param prefix an optional prefix to prepend the log message
     */
    private void logPoolState(String... prefix) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} db instance[{}] stats (total={}, active={}, idle={}, idleTest={} waiting={})",
                    (prefix.length > 0 ? prefix[0] : ""), config.getInstanceName(),
                    allConnections.size() - getCount(STATE_REMOVED), getCount(STATE_IN_USE), getCount(STATE_NOT_IN_USE), getCount(STATE_HEARTBEAT), getThreadsAwaitingConnection());
        }
    }

    /**
     * The idle object evictor.
     */
    class Evictor implements Runnable {

        private ScheduledFuture<?> scheduledFuture;

        /**
         * Run pool maintenance.  Evict objects qualifying for eviction and then
         * ensure that the minimum number of idle instances are available.
         * Since the Timer that invokes Evictors is shared for all Pools but
         * pools may exist in different class loaders, the Evictor ensures that
         * any actions taken are under the class loader of the factory
         * associated with the pool.
         */
        @Override
        public void run() {
            if (instance.skipEvit()) {
                return;
            }

            final ClassLoader savedClassLoader =
                    Thread.currentThread().getContextClassLoader();
            try {
                if (factoryClassLoader != null) {
                    // Set the class loader for the factory
                    final ClassLoader cl = factoryClassLoader.get();
                    if (cl == null) {
                        // The pool has been dereferenced and the class loader
                        // GC'd. Cancel this timer so the pool can be GC'd as
                        // well.
                        cancel();
                        return;
                    }
                    Thread.currentThread().setContextClassLoader(cl);
                }

                // Evict from the pool
                evict();

                // Try to maintain minimum connections
                fillPool();
            } finally {
                // Restore the previous CCL
                Thread.currentThread().setContextClassLoader(savedClassLoader);
            }
        }

        /**
         * Sets the scheduled future.
         *
         * @param scheduledFuture the scheduled future.
         */
        void setScheduledFuture(final ScheduledFuture<?> scheduledFuture) {
            this.scheduledFuture = scheduledFuture;
        }

        /**
         * Cancels the scheduled future.
         */
        void cancel() {
            scheduledFuture.cancel(false);
        }
    }
}
