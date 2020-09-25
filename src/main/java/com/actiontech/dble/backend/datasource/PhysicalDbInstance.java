/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.ConnectionPool;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.factory.MySQLConnectionFactory;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.Scheduler;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.RW_SPLIT_OFF;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class PhysicalDbInstance implements ReadTimeStatusInstance {

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
    private volatile boolean needSkipEvit = false;


    public PhysicalDbInstance(DbInstanceConfig config, DbGroupConfig dbGroupConfig, boolean isReadNode) {
        this.config = config;
        this.name = config.getInstanceName();
        this.dbGroupConfig = dbGroupConfig;
        this.heartbeat = new MySQLHeartbeat(this);
        this.readInstance = isReadNode;
        this.disabled = new AtomicBoolean(config.isDisabled());
        this.connectionPool = new ConnectionPool(config, this, new MySQLConnectionFactory());
    }

    public PhysicalDbInstance(PhysicalDbInstance org) {
        this.config = org.config;
        this.name = org.name;
        this.dbGroupConfig = org.dbGroupConfig;
        this.readInstance = org.readInstance;
        this.disabled = new AtomicBoolean(org.disabled.get());
    }

    public void init(String reason) {
        init(reason, true);
    }

    public void init(String reason, boolean isInitHeartbeat) {
        if (disabled.get() || fakeNode) {
            LOGGER.info("init dbInstance[{}] because {}, but it is disabled or a fakeNode, skip initialization.", this.dbGroup.getGroupName() + "." + name, reason);
            return;
        }

        if (!isInitial.compareAndSet(false, true)) {
            LOGGER.info("init dbInstance[{}] because {}, but it has been initialized, skip initialization.", this.dbGroup.getGroupName() + "." + name, reason);
            return;
        }

        int size = config.getMinCon();
        String[] physicalSchemas = dbGroup.getSchemas();
        int initSize = physicalSchemas.length;
        if (size < initSize) {
            LOGGER.warn("For db instance[{}], minIdle is less than (the count of schema), so dble will create at least 1 conn for every schema, " +
                    "minCon size before:{}, now:{}", this.dbGroup.getGroupName() + "." + name, size, initSize);
            config.setMinCon(initSize);
        }

        size = config.getMaxCon();
        if (size < initSize) {
            LOGGER.warn("For db instance[{}], maxTotal[{}] is less than the initSize of dataHost,change the maxCon into {}", this.dbGroup.getGroupName() + "." + name, size, initSize);
            config.setMaxCon(initSize);
        }

        LOGGER.info("init dbInstance[{}]", this.dbGroup.getGroupName() + "." + name);
        start(reason, isInitHeartbeat);
    }

    public void createConnectionSkipPool(String schema, ResponseHandler handler) {
        connectionPool.newConnection(schema, handler);
    }

    public void getConnection(final String schema, final ResponseHandler handler,
                              final Object attachment, boolean mustWrite) throws IOException {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-connection-from-db-instance");
        AbstractService service = TraceManager.getThreadService();
        try {
            if (mustWrite && readInstance) {
                throw new IOException("primary dbInstance switched");
            }

            BackendConnection con = (BackendConnection) connectionPool.borrowDirectly(schema);
            if (con != null) {
                if (!StringUtil.equals(con.getSchema(), schema)) {
                    // need do sharding syn in before sql send
                    con.setSchema(schema);
                }
                TraceManager.crossThread(con.getBackendService(), "backend-response-service", service);
                con.getBackendService().setAttachment(attachment);
                handler.connectionAcquired(con);
                return;
            }

            DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
                BackendConnection con1;
                try {
                    con1 = getConnection(schema, config.getPoolConfig().getConnectionTimeout());
                } catch (IOException e) {
                    handler.connectionError(e, attachment);
                    return;
                }
                if (!StringUtil.equals(con1.getSchema(), schema)) {
                    // need do sharding syn in before sql send
                    con1.setSchema(schema);
                }
                TraceManager.crossThread(con1.getBackendService(), "backend-response-service", service);
                con1.getBackendService().setAttachment(attachment);
                handler.connectionAcquired(con1);
            });
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    // execute in complex executor guard by business executor
    public BackendConnection getConnection(String schema, final Object attachment) throws IOException {
        BackendConnection con = getConnection(schema, config.getPoolConfig().getConnectionTimeout());
        if (!StringUtil.equals(con.getSchema(), schema)) {
            // need do sharding syn in before sql send
            con.setSchema(schema);
        }
        ((MySQLResponseService) con.getService()).setAttachment(attachment);
        return con;
    }

    private BackendConnection getConnection(final String schema, final long hardTimeout) throws IOException {
        if (this.connectionPool == null) {
            throw new IOException("connection pool isn't initialized");
        }

        if (disabled.get()) {
            throw new IOException("the dbInstance[" + this.dbGroup.getGroupName() + "." + name + "] is disabled.");
        }

        final long startTime = System.currentTimeMillis();

        try {
            long timeout = hardTimeout;
            do {
                final BackendConnection conn = (BackendConnection) this.connectionPool.borrow(schema, timeout, MILLISECONDS);
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
            throw new IOException(this.dbGroup.getGroupName() + "." + name + " - Interrupted during connection acquisition", e);
        }

        throw new IOException(this.dbGroup.getGroupName() + "." + name + " - Connection is not available, request timed out after " + (System.currentTimeMillis() - startTime) + "ms.");
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
            LOGGER.info("the instance[{}] is disabled or fake node, skip to start heartbeat.", this.dbGroup.getGroupName() + "." + name);
            return;
        }

        heartbeat.start();
        heartbeat.setScheduledFuture(Scheduler.getInstance().getScheduledExecutor().scheduleAtFixedRate(() -> {
            if (DbleServer.getInstance().getConfig().isFullyConfigured()) {
                if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
                    return;
                }

                heartbeat.heartbeat();
            }
        }, 0L, config.getPoolConfig().getHeartbeatPeriodMillis(), TimeUnit.MILLISECONDS));
    }

    public void start(String reason) {
        start(reason, true);
    }

    public void start(String reason, boolean isStartHeartbeat) {
        if ((dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF || dbGroup.getWriteDbInstance() == this) && !dbGroup.isUseless()) {
            LOGGER.info("start connection pool of physical db instance[{}], due to {}", this.dbGroup.getGroupName() + "." + name, reason);
            this.connectionPool.startEvictor();
        }
        if (isStartHeartbeat) {
            startHeartbeat();
        }
    }

    public void stop(String reason, boolean closeFront) {
        stop(reason, closeFront, true);
    }

    public void stop(String reason, boolean closeFront, boolean isStopHeartbeat) {
        if (isStopHeartbeat) {
            heartbeat.stop(reason);
        }
        if (dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF || dbGroup.getWriteDbInstance() == this) {
            LOGGER.info("stop connection pool of physical db instance[{}], due to {}", this.dbGroup.getGroupName() + "." + name, reason);
            connectionPool.stop(reason, closeFront);
        }
        isInitial.set(false);
    }

    public void closeAllConnection(String reason) {
        this.needSkipEvit = true;
        this.connectionPool.forceCloseAllConnection(reason);
        this.needSkipEvit = false;
    }

    public boolean isAlive() {
        return !disabled.get() && !isFakeNode() && heartbeat.isHeartBeatOK();
    }

    public boolean skipEvit() {
        return !isAlive() && needSkipEvit;
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
            start("execute manager cmd of enable");
            return true;
        }
        return false;
    }

    public final int getActiveConnections() {
        return connectionPool.getCount(PooledConnection.STATE_IN_USE);
    }

    public final int getActiveConnections(String schema) {
        return connectionPool.getCount(schema, PooledConnection.STATE_IN_USE);
    }

    public final int getIdleConnections() {
        return connectionPool.getCount(PooledConnection.STATE_NOT_IN_USE);
    }

    public final int getIdleConnections(String schema) {
        return connectionPool.getCount(schema, PooledConnection.STATE_NOT_IN_USE);
    }

    public final int getTotalConnections() {
        return connectionPool.size() - connectionPool.getCount(PooledConnection.STATE_REMOVED);
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
        DbInstanceConfig otherConfig = dbInstance.getConfig();
        DbInstanceConfig thisConfig = this.getConfig();
        return otherConfig.getUser().equals(thisConfig.getUser()) && otherConfig.getUrl().equals(thisConfig.getUrl()) &&
                otherConfig.getMaxCon() == thisConfig.getMaxCon() && otherConfig.getMinCon() == thisConfig.getMinCon() &&
                otherConfig.getPassword().equals(thisConfig.getPassword()) && otherConfig.getInstanceName().equals(thisConfig.getInstanceName()) &&
                dbInstance.isDisabled() == this.isDisabled() && otherConfig.getReadWeight() == thisConfig.getReadWeight() &&
                otherConfig.getPoolConfig().equals(thisConfig.getPoolConfig()) && otherConfig.isUsingDecrypt() == thisConfig.isUsingDecrypt();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "dbInstance[name=" + name +
                ",disabled=" + disabled.toString() +
                ",maxCon=" + config.getMaxCon() +
                ",minCon=" + config.getMinCon() + "]";
    }

}
