/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.delyDetection.DelayDetection;
import com.actiontech.dble.backend.delyDetection.DelayDetectionStatus;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.ConnectionPool;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.factory.MySQLConnectionFactory;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.RW_SPLIT_OFF;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class PhysicalDbInstance implements ReadTimeStatusInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbInstance.class);

    private final String name;
    private DbInstanceConfig config;
    private volatile boolean readInstance;

    private DbGroupConfig dbGroupConfig;
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
    private volatile DelayDetectionStatus delayDetectionStatus = DelayDetectionStatus.STOP;
    protected DelayDetection delayDetection;


    private final AtomicBoolean isInitial = new AtomicBoolean(false);

    // connection pool
    private ConnectionPool connectionPool;
    protected MySQLHeartbeat heartbeat;
    private volatile boolean needSkipEvit = false;
    private volatile boolean needSkipHeartTest = false;
    private volatile int logCount;


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

    public void init(String reason, boolean isInitHeartbeat, boolean delayDetectionStart) {
        if (disabled.get() || fakeNode) {
            LOGGER.info("init dbInstance[{}] because {}, but it is disabled or a fakeNode, skip initialization.", this.dbGroup.getGroupName() + "." + name, reason);
            return;
        }

        if (!isInitial.compareAndSet(false, true)) {
            LOGGER.info("init dbInstance[{}] because {}, but it has been initialized, skip initialization.", this.dbGroup.getGroupName() + "." + name, reason);
            return;
        }
        //minCon/maxCon/numOfShardingNodes
        checkPoolSize();

        LOGGER.info("init dbInstance[{}]", this.dbGroup.getGroupName() + "." + name);
        if (delayDetectionStart) {
            delayDetection = new DelayDetection(this);
        }
        start(reason, isInitHeartbeat, delayDetectionStart);
    }

    protected void checkPoolSize() {
        int size = config.getMinCon();
        List<String> physicalSchemas = dbGroup.getSchemas();
        int initSize = physicalSchemas.size();
        if (size < initSize) {
            LOGGER.warn("For db instance[{}], minIdle is less than (the count of shardingNodes), so dble will create at least 1 conn for every schema, " +
                    "minCon size before:{}, now:{}", this.dbGroup.getGroupName() + "." + name, size, initSize);
            config.setMinCon(initSize);
        }

        initSize = Math.max(initSize, config.getMinCon());
        size = config.getMaxCon();
        if (size < initSize) {
            LOGGER.warn("For db instance[{}], maxTotal[{}] is less than the minCon or the count of shardingNodes,change the maxCon into {}", this.dbGroup.getGroupName() + "." + name, size, initSize);
            config.setMaxCon(initSize);
        }
    }

    public void createConnectionSkipPool(String schema, ResponseHandler handler) {
        connectionPool.newConnection(schema, handler);
    }

    public void syncGetConnection(final String schema, final ResponseHandler handler,
                                  final Object attachment, boolean mustWrite) throws IOException {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-connection-from-db-instance");
        AbstractService service = TraceManager.getThreadService();
        try {
            if (mustWrite && readInstance) {
                throw new IOException("primary dbInstance switched");
            }
            BackendConnection con;
            con = (BackendConnection) connectionPool.borrowDirectly(schema);
            if (con == null) {
                con = getConnection(schema, config.getPoolConfig().getConnectionTimeout());
            }
            if (!StringUtil.equals(con.getSchema(), schema)) {
                // need do sharding syn in before sql send
                con.setSchema(schema);
            }
            TraceManager.crossThread(con.getBackendService(), "backend-response-service", service);
            con.getBackendService().setAttachment(attachment);
            handler.connectionAcquired(con);
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void getConnection(final String schema, final ResponseHandler handler,
                              final Object attachment, boolean mustWrite) throws IOException {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-connection-from-db-instance");
        AbstractService service = TraceManager.getThreadService();
        try {
            if (mustWrite && readInstance) {
                throw new IOException("primary dbInstance switched");
            }

            if (!config.getPoolConfig().getTestOnBorrow()) {
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

    public void setDbGroupConfig(DbGroupConfig dbGroupConfig) {
        this.dbGroupConfig = dbGroupConfig;
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
        if (dbGroup.isDelayDetectionStart()) {
            DelayDetectionStatus status = getDelayDetectionStatus();
            if (status == DelayDetectionStatus.ERROR || status == DelayDetectionStatus.TIMEOUT) {
                return false;
            }
            return true;
        }
        Integer slaveBehindMaster = heartbeat.getSlaveBehindMaster();
        int dbSynStatus = heartbeat.getDbSynStatus();
        if (slaveBehindMaster == null || dbSynStatus == MySQLHeartbeat.DB_SYN_ERROR) {
            return false;
        }
        boolean isSync = dbSynStatus == MySQLHeartbeat.DB_SYN_NORMAL;
        boolean isNotDelay = slaveBehindMaster <= this.dbGroupConfig.getDelayThreshold();
        return isSync && isNotDelay;
    }

    public void startHeartbeat() {
        if (this.isDisabled() || this.isFakeNode()) {
            LOGGER.info("the instance[{}] is disabled or fake node, skip to start heartbeat.", this.dbGroup.getGroupName() + "." + name);
            return;
        }

        heartbeat.start(config.getPoolConfig().getHeartbeatPeriodMillis());
    }

    public void startDelayDetection() {
        if (this.isDisabled() || this.isFakeNode()) {
            LOGGER.info("the instance[{}] is disabled or fake node, skip to start delayDetection.", this.dbGroup.getGroupName() + "." + name);
            return;
        }
        if (!dbGroup.isDelayDetectionStart()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("this instance does not require delay detection to be enabled");
            }
            return;
        }
        long initialDelay = 0;
        if (readInstance) {
            initialDelay = dbGroupConfig.getDelayPeriodMillis();
        }
        delayDetection.start(initialDelay);
    }

    public void start() {
        if (this.isDisabled() || this.isFakeNode()) {
            LOGGER.info("the instance[{}] is disabled or fake node, skip to start heartbeat.", this.dbGroup.getGroupName() + "." + name);
            return;
        }

        heartbeat.start(config.getPoolConfig().getHeartbeatPeriodMillis());
    }

    void start(String reason, boolean isStartHeartbeat, boolean delayDetectionStart) {
        startPool(reason);
        if (isStartHeartbeat) {
            startHeartbeat();
        }
        if (delayDetectionStart && dbGroup.isDelayDetectionStart()) {
            startDelayDetection();
        }
    }

    public void startPool(String reason) {
        if (disabled.get() || fakeNode) {
            LOGGER.info("init dbInstance[{}] because {}, but it is disabled or a fakeNode, skip initialization.", this.dbGroup.getGroupName() + "." + name, reason);
            return;
        }
        if ((dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF || dbGroup.getWriteDbInstance() == this) && !dbGroup.isUseless()) {
            this.connectionPool.startEvictor(this.dbGroup.getGroupName() + "." + name, reason);
        }
    }

    private boolean checkState() {
        if (dbGroup.getBindingCount() != 0) {
            dbGroup.setState(PhysicalDbGroup.STATE_DELETING);
            IOProcessor.BACKENDS_OLD_INSTANCE.add(this);
            return false;
        }
        if (dbGroup.isStop()) {
            return false;
        }
        if (dbGroup.getBindingCount() != 0) {
            dbGroup.setState(PhysicalDbGroup.STATE_DELETING);
            IOProcessor.BACKENDS_OLD_INSTANCE.add(this);
            return false;
        }
        return true;
    }

    public boolean stopOfBackground(String reason) {
        if (dbGroup.getState() == PhysicalDbGroup.STATE_DELETING && dbGroup.getBindingCount() == 0) {
            stopDirectly(reason, false, false);
            return true;
        }
        return false;
    }

    public void stopDirectly(String reason, boolean closeFront, boolean isStopPool) {
        stop(reason, closeFront, true, dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF || dbGroup.getWriteDbInstance() == this || isStopPool, true);
    }

    public void stop(String reason, boolean closeFront, boolean isStopPool) {
        boolean flag = checkState();
        if (!flag) {
            return;
        }
        stopDirectly(reason, closeFront, isStopPool);
    }

    public void stop(String reason, boolean closeFront) {
        boolean flag = checkState();
        if (!flag) {
            return;
        }
        stopDirectly(reason, closeFront, false);
    }

    protected void stop(String reason, boolean closeFront, boolean isStopHeartbeat, boolean isStopPool, boolean delayDetectionStop) {
        if (isStopHeartbeat) {
            stopHeartbeat(reason);
        }
        if (delayDetectionStop) {
            stopDelayDetection(reason);
        }
        if (isStopPool) {
            stopPool(reason, closeFront);
        }
        isInitial.set(false);
    }

    public void stopHeartbeat(String reason) {
        if (LOGGER.isDebugEnabled()) {
            ReloadLogHelper.debug("stop heartbeat :{},reason:{}", this.toString(), reason);
        }
        heartbeat.stop(reason);
    }

    public void stopDelayDetection(String reason) {
        if (LOGGER.isDebugEnabled()) {
            ReloadLogHelper.debug("stop delayDetection :{},reason:{}", this.toString(), reason);
        }
        if (Objects.nonNull(delayDetection)) {
            delayDetection.stop(reason);
        }
    }

    public void stopPool(String reason, boolean closeFront) {
        LOGGER.info("stop connection pool of physical db instance[{}], due to {}", this.dbGroup.getGroupName() + "." + name, reason);
        connectionPool.stop(reason, closeFront);
    }

    public void updatePoolCapacity() {
        //minCon/maxCon/numOfShardingNodes
        if ((dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF || dbGroup.getWriteDbInstance() == this) && !dbGroup.isUseless()) {
            checkPoolSize();
            connectionPool.evictImmediately();
            connectionPool.fillPool();
        }
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
            start("execute manager cmd of enable", true, true);
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

    public boolean isNeedSkipHeartTest() {
        return needSkipHeartTest;
    }

    public void setNeedSkipHeartTest(boolean needSkipHeartTest) {
        this.needSkipHeartTest = needSkipHeartTest;
    }

    public final int getTotalConnections() {
        return connectionPool.size() - connectionPool.getCount(PooledConnection.STATE_REMOVED);
    }

    public int getLogCount() {
        return logCount;
    }

    public void setLogCount(int logCount) {
        this.logCount = logCount;
    }

    public DelayDetectionStatus getDelayDetectionStatus() {
        return delayDetectionStatus;
    }

    public void setDelayDetectionStatus(DelayDetectionStatus delayDetectionStatus) {
        this.delayDetectionStatus = delayDetectionStatus;
    }

    public DelayDetection getDelayDetection() {
        return delayDetection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhysicalDbInstance that = (PhysicalDbInstance) o;

        return readInstance == that.readInstance &&
                Objects.equals(name, that.name) &&
                Objects.equals(config, that.config) &&
                dbGroupConfig.equalsBaseInfo(that.dbGroupConfig) &&
                dbGroup.equalsBaseInfo(that.dbGroup) &&
                Objects.equals(disabled.get(), that.disabled.get());
    }


    @Override
    public int hashCode() {
        return Objects.hash(name, config, readInstance, dbGroupConfig, dbGroup, disabled, heartbeat);
    }

    public boolean equalsForConnectionPool(PhysicalDbInstance dbInstance) {
        return this.config.getUrl().equals(dbInstance.getConfig().getUrl()) &&
                this.config.getPort() == dbInstance.getConfig().getPort() &&
                this.config.getUser().equals(dbInstance.getConfig().getUser()) &&
                this.config.getPassword().equals(dbInstance.getConfig().getPassword()) &&
                this.config.isUsingDecrypt() == dbInstance.getConfig().isUsingDecrypt() &&
                this.config.getPoolConfig().getTimeBetweenEvictionRunsMillis() == dbInstance.getConfig().getPoolConfig().getTimeBetweenEvictionRunsMillis() &&
                this.disabled.get() == dbInstance.isDisabled();
    }

    public boolean equalsForPoolCapacity(PhysicalDbInstance dbInstance) {
        return this.config.getMinCon() == dbInstance.getConfig().getMinCon() &&
                this.config.getMaxCon() == dbInstance.getConfig().getMaxCon();
    }

    public boolean equalsForHeartbeat(PhysicalDbInstance dbInstance) {
        return this.config.getUrl().equals(dbInstance.getConfig().getUrl()) &&
                this.config.getPort() == dbInstance.getConfig().getPort() &&
                this.config.getUser().equals(dbInstance.getConfig().getUser()) &&
                this.config.getPassword().equals(dbInstance.getConfig().getPassword()) &&
                this.config.isUsingDecrypt() == dbInstance.getConfig().isUsingDecrypt() &&
                this.config.getPoolConfig().getHeartbeatPeriodMillis() == dbInstance.getConfig().getPoolConfig().getHeartbeatPeriodMillis() &&
                this.disabled.get() == dbInstance.isDisabled();
    }

    public boolean equalsForDelayDetection(PhysicalDbInstance dbInstance) {
        return this.config.getUrl().equals(dbInstance.getConfig().getUrl()) &&
                this.config.getPort() == dbInstance.getConfig().getPort() &&
                this.config.getUser().equals(dbInstance.getConfig().getUser()) &&
                this.config.getPassword().equals(dbInstance.getConfig().getPassword()) &&
                this.config.isUsingDecrypt() == dbInstance.getConfig().isUsingDecrypt() &&
                this.disabled.get() == dbInstance.isDisabled();
    }

    public boolean equalsForTestConn(PhysicalDbInstance dbInstance) {
        return this.config.getUrl().equals(dbInstance.getConfig().getUrl()) &&
                this.config.getPort() == dbInstance.getConfig().getPort() &&
                this.config.getUser().equals(dbInstance.getConfig().getUser()) &&
                this.config.getPassword().equals(dbInstance.getConfig().getPassword()) &&
                this.config.isUsingDecrypt() == dbInstance.getConfig().isUsingDecrypt() &&
                this.disabled.get() == dbInstance.isDisabled();
    }

    @Override
    public String toString() {
        return "dbInstance[name=" + name +
                ",disabled=" + disabled.toString() +
                ",maxCon=" + config.getMaxCon() +
                ",minCon=" + config.getMinCon() + "]";
    }

    public void copyBaseInfo(PhysicalDbInstance physicalDbInstance) {
        this.config = physicalDbInstance.getConfig();
        this.connectionPool.copyBaseInfo(physicalDbInstance.connectionPool);
    }

}
