/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.ConnectionPool;
import com.actiontech.dble.backend.pool.PooledEntry;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.singleton.Scheduler;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class PhysicalDbInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbInstance.class);

    private final String name;
    private final DbInstanceConfig config;
    private volatile boolean readInstance;

    private final DbGroupConfig dbGroupConfig;
    private PhysicalDbGroup dbGroup;
    private final AtomicBoolean disabled;
    private String dsVersion;
    private volatile boolean autocommitSynced;
    private volatile boolean isolationSynced;
    private volatile boolean testConnSuccess = false;
    private volatile boolean readOnly = false;
    private volatile boolean fakeNode = false;
    private final LongAdder readCount = new LongAdder();
    private final LongAdder writeCount = new LongAdder();

    private final AtomicBoolean isInitial = new AtomicBoolean(false);

    // connection pool
    private ConnectionPool connectionPool;
    protected MySQLHeartbeat heartbeat;
    private volatile long heartbeatRecoveryTime;


    public PhysicalDbInstance(DbInstanceConfig config, DbGroupConfig dbGroupConfig, boolean isReadNode) {
        this.config = config;
        this.name = config.getInstanceName();
        this.dbGroupConfig = dbGroupConfig;
        this.heartbeat = new MySQLHeartbeat(this);
        this.readInstance = isReadNode;
        this.disabled = new AtomicBoolean(config.isDisabled());
        this.connectionPool = new ConnectionPool(config, this);
    }

    public PhysicalDbInstance(PhysicalDbInstance org) {
        this.config = org.config;
        this.name = org.name;
        this.dbGroupConfig = org.dbGroupConfig;
        this.readInstance = org.readInstance;
        this.disabled = new AtomicBoolean(org.disabled.get());
    }

    public void init() {
        if (disabled.get() || fakeNode) {
            LOGGER.info("{} is disabled or a fakeNode, skip initialization", name);
            return;
        }

        if (!isInitial.compareAndSet(false, true)) {
            LOGGER.info("{} has been initialized, skip", name);
            return;
        }

        int size = config.getMinCon();
        String[] physicalSchemas = dbGroup.getSchemas();
        int initSize = physicalSchemas.length;
        if (size < initSize) {
            LOGGER.warn("For db instance[{}], minIdle is less than (the count of schema), so dble will create at least 1 conn for every schema, " +
                    "minCon size before:{}, now:{}", name, size, initSize);
            config.setMinCon(initSize);
        }

        size = config.getMaxCon();
        if (size < initSize) {
            LOGGER.warn("For db instance[{}], maxTotal[{}] is less than the initSize of dataHost,change the maxCon into {}", name, size, initSize);
            config.setMaxCon(initSize);
        }

        start("initial");
    }

    public void createConnectionSkipPool(String schema, ResponseHandler handler) {
        connectionPool.newConnection(schema, handler);
    }

    public void getConnection(String schema, final ResponseHandler handler,
                              final Object attachment, boolean mustWrite) throws IOException {

        if (mustWrite && readInstance) {
            throw new IOException("primary dbInstance switched");
        }

        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                BackendConnection con;
                try {
                    con = getConnection(schema, config.getPoolConfig().getConnectionTimeout());
                } catch (IOException e) {
                    handler.connectionError(e, attachment);
                    return;
                }
                con.setAttachment(attachment);
                handler.connectionAcquired(con);
            }
        });
    }

    // execute in complex executor guard by business executor
    public BackendConnection getConnection(String schema, final Object attachment) throws IOException {
        BackendConnection con = getConnection(schema, config.getPoolConfig().getConnectionTimeout());
        con.setAttachment(attachment);
        return con;
    }

    public BackendConnection getConnection(final String schema, final long hardTimeout) throws IOException {
        if (this.connectionPool == null) {
            throw new IOException("connection pool isn't initalized");
        }

        if (disabled.get()) {
            throw new IOException("the dbInstance[" + name + "] is disabled.");
        }

        final long startTime = System.currentTimeMillis();

        try {
            long timeout = hardTimeout;
            do {
                final BackendConnection conn = this.connectionPool.borrow(schema, timeout, MILLISECONDS);
                if (conn == null) {
                    break; // We timed out... break and throw exception
                }

                final long now = System.currentTimeMillis();
                if (config.getPoolConfig().getTestOnBorrow()) {
                    ConnectionHeartBeatHandler heartBeatHandler = new ConnectionHeartBeatHandler(conn, true, connectionPool);
                    boolean isFinished = heartBeatHandler.ping(config.getPoolConfig().getConnectionHeartbeatTimeout());
                    if (!isFinished) {
                        conn.close("connection test fail after create"); // Throw away the dead connection (passed max age or failed alive test)
                        timeout = hardTimeout - (now - startTime);
                        continue;
                    }
                }

                if (!StringUtil.equals(conn.getSchema(), schema)) {
                    // need do sharding syn in before sql send
                    conn.setSchema(schema);
                }
                return conn;

            } while (timeout > 0L);
        } catch (InterruptedException e) {
            throw new IOException(name + " - Interrupted during connection acquisition", e);
        }

        throw new IOException(name + " - Connection is not available, request timed out after " + (System.currentTimeMillis() - startTime) + "ms.");
    }

    public void release(BackendConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("release {}", connection);
        }
        connectionPool.release(connection);
    }

    public void close(BackendConnection connection) {
        connectionPool.close(connection);
    }

    public void setTestConnSuccess(boolean testConnSuccess) {
        this.testConnSuccess = testConnSuccess;
    }

    public boolean isTestConnSuccess() {
        return testConnSuccess;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

    public long getCount(boolean isRead) {
        if (isRead) {
            return readCount.longValue();
        }
        return writeCount.longValue();
    }

    public void incrementReadCount() {
        readCount.increment();
    }

    public void incrementWriteCount() {
        writeCount.increment();
    }

    public DbGroupConfig getDbGroupConfig() {
        return dbGroupConfig;
    }


    public boolean isFakeNode() {
        return fakeNode;
    }

    public void setFakeNode(boolean fakeNode) {
        this.fakeNode = fakeNode;
    }

    public boolean isReadInstance() {
        return readInstance;
    }

    void setReadInstance(boolean value) {
        this.readInstance = value;
    }

    public void setDbGroup(PhysicalDbGroup dbGroup) {
        this.dbGroup = dbGroup;
    }

    public PhysicalDbGroup getDbGroup() {
        return dbGroup;
    }

    public boolean isAutocommitSynced() {
        return autocommitSynced;
    }

    public void setAutocommitSynced(boolean autocommitSynced) {
        this.autocommitSynced = autocommitSynced;
    }

    public boolean isIsolationSynced() {
        return isolationSynced;
    }

    public void setIsolationSynced(boolean isolationSynced) {
        this.isolationSynced = isolationSynced;
    }

    public String getDsVersion() {
        return dsVersion;
    }

    protected void setDsVersion(String dsVersion) {
        this.dsVersion = dsVersion;
    }

    public String getName() {
        return name;
    }

    public MySQLHeartbeat getHeartbeat() {
        return heartbeat;
    }

    public boolean isSalveOrRead() {
        if (dbGroup != null) {
            return dbGroup.isSlave(this) || this.readInstance;
        } else {
            return this.readInstance;
        }
    }

    /**
     * used for init or reload
     */
    public abstract boolean testConnection() throws IOException;

    public DbInstanceConfig getConfig() {
        return config;
    }

    boolean canSelectAsReadNode() {
        Integer slaveBehindMaster = heartbeat.getSlaveBehindMaster();
        int dbSynStatus = heartbeat.getDbSynStatus();
        if (slaveBehindMaster == null || dbSynStatus == MySQLHeartbeat.DB_SYN_ERROR) {
            return false;
        }
        boolean isSync = dbSynStatus == MySQLHeartbeat.DB_SYN_NORMAL;
        boolean isNotDelay = slaveBehindMaster < this.dbGroupConfig.getDelayThreshold();
        return isSync && isNotDelay;
    }

    private void startHeartbeat() {
        if (this.isDisabled() || this.isFakeNode()) {
            LOGGER.info("the instance[{}] is disabled or fake node, skip to start heartbeat.", name);
            return;
        }

        heartbeat.start();
        heartbeat.setScheduledFuture(Scheduler.getInstance().getScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DbleServer.getInstance().getConfig().isFullyConfigured()) {
                    if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
                        return;
                    }

                    heartbeat.heartbeat();
                }
            }
        }, 0L, config.getPoolConfig().getHeartbeatPeriodMillis(), TimeUnit.MILLISECONDS));
    }

    public void start(String reason) {
        LOGGER.info("start connection pool of physical db instance[{}], due to {}", name, reason);
        this.connectionPool.startEvictor();
        startHeartbeat();
    }

    public void stop(String reason, boolean closeFront) {
        heartbeat.stop(reason);
        connectionPool.stop(reason, closeFront);
    }

    public void closeAllConnection(String reason) {
        this.connectionPool.closeAllConnections(reason);
    }

    public boolean isAlive() {
        return !disabled.get() && !isFakeNode() && heartbeat.isHeartBeatOK();
    }

    public boolean isDisabled() {
        return disabled.get();
    }

    public void setDisabled(boolean isDisabled) {
        disabled.set(isDisabled);
    }

    public boolean disable(String reason) {
        if (disabled.compareAndSet(false, true)) {
            stop(reason, false);
            return true;
        }
        return false;
    }

    public boolean enable() {
        if (disabled.compareAndSet(true, false)) {
            start("execute manger cmd of enable");
            return true;
        }
        return false;
    }

    public final int getActiveConnections() {
        return connectionPool.getCount(PooledEntry.STATE_IN_USE);
    }

    public final int getActiveConnections(String schema) {
        return connectionPool.getCount(schema, PooledEntry.STATE_IN_USE);
    }

    public final int getIdleConnections() {
        return connectionPool.getCount(PooledEntry.STATE_NOT_IN_USE);
    }

    public final int getIdleConnections(String schema) {
        return connectionPool.getCount(schema, PooledEntry.STATE_NOT_IN_USE);
    }

    public final int getTotalConnections() {
        return connectionPool.size() - connectionPool.getCount(PooledEntry.STATE_REMOVED);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PhysicalDbInstance)) {
            return false;
        }

        PhysicalDbInstance dbInstance = (PhysicalDbInstance) other;
        DbInstanceConfig dbInstanceConfig = dbInstance.getConfig();
        return dbInstanceConfig.getUser().equals(this.getConfig().getUser()) && dbInstanceConfig.getUrl().equals(this.getConfig().getUrl()) &&
                dbInstanceConfig.getPassword().equals(this.getConfig().getPassword()) && dbInstanceConfig.getInstanceName().equals(this.getConfig().getInstanceName()) &&
                dbInstance.isDisabled() == this.isDisabled() && dbInstanceConfig.getReadWeight() == this.getConfig().getReadWeight() &&
                dbInstanceConfig.getPoolConfig().equals(this.getConfig().getPoolConfig());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "dbInstance[name=" + name +
                ",disabled=" +
                disabled.toString() + ",maxCon=" +
                config.getMaxCon() + "]";
    }

}
