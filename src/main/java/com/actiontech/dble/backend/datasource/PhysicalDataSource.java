/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.ConMap;
import com.actiontech.dble.backend.ConQueue;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.actiontech.dble.backend.mysql.nio.handler.DelegateResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.NewConnectionRespHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.config.model.DataSourceConfig;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PhysicalDataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDataSource.class);

    private final String name;
    private int size;
    private final DataSourceConfig config;
    private final ConMap conMap = new ConMap();
    private MySQLHeartbeat heartbeat;
    private volatile boolean readNode;
    private volatile long heartbeatRecoveryTime;
    private final DataHostConfig hostConfig;
    private PhysicalDataHost dataHost;
    private final AtomicInteger connectionCount;
    private volatile AtomicBoolean disabled;
    private volatile boolean autocommitSynced = false;
    private volatile boolean isolationSynced = false;
    private volatile boolean testConnSuccess = false;
    private volatile boolean readOnly = false;
    private AtomicLong readCount = new AtomicLong(0);
    private AtomicLong writeCount = new AtomicLong(0);
    private String dsVersion;

    public PhysicalDataSource(DataSourceConfig config, DataHostConfig hostConfig, boolean isReadNode) {
        this.size = config.getMaxCon();
        this.config = config;
        this.name = config.getHostName();
        this.hostConfig = hostConfig;
        heartbeat = this.createHeartBeat();
        this.readNode = isReadNode;
        this.connectionCount = new AtomicInteger();
        this.disabled = new AtomicBoolean(config.isDisabled());
    }

    public PhysicalDataSource(PhysicalDataSource org) {
        this.size = org.size;
        this.config = org.config;
        this.name = org.name;
        this.hostConfig = org.hostConfig;
        this.readNode = org.readNode;
        this.connectionCount = org.connectionCount;
        this.disabled = new AtomicBoolean(org.disabled.get());
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

    public long getReadCount() {
        return readCount.get();
    }

    void setReadCount() {
        readCount.addAndGet(1);
    }

    public long getWriteCount() {
        return writeCount.get();
    }

    void setWriteCount() {
        writeCount.addAndGet(1);
    }

    public DataHostConfig getHostConfig() {
        return hostConfig;
    }

    public boolean isReadNode() {
        return readNode;
    }

    void setReadNode(boolean value) {
        this.readNode = value;
    }

    public int getSize() {
        return size;
    }

    public void setDataHost(PhysicalDataHost dataHost) {
        this.dataHost = dataHost;
    }

    public PhysicalDataHost getDataHost() {
        return dataHost;
    }

    public abstract MySQLHeartbeat createHeartBeat();

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

    public void setSize(int size) {
        this.size = size;
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

    public long getExecuteCount() {
        long executeCount = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            executeCount += queue.getExecuteCount();

        }
        return executeCount;
    }

    public long getExecuteCountForSchema(String schema) {
        ConQueue queue = conMap.getSchemaConQueue(schema);
        return queue == null ? 0 : queue.getExecuteCount();

    }

    public int getActiveCountForSchema(String schema) {
        return conMap.getActiveCountForSchema(schema, this);
    }

    public int getIdleCountForSchema(String schema) {
        ConQueue queue = conMap.getSchemaConQueue(schema);
        if (queue == null) {
            return 0;
        } else {
            return queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        }
    }

    public MySQLHeartbeat getHeartbeat() {
        return heartbeat;
    }

    public int getIdleCount() {
        int total = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        }
        return total;
    }

    public boolean isSalveOrRead() {
        if (dataHost != null) {
            return dataHost.isSlave(this) || this.readNode;
        } else {
            return this.readNode;
        }
    }

    void connectionHeatBeatCheck(long conHeartBeatPeriod) {

        long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;

        for (ConQueue queue : conMap.getAllConQueue()) {
            longIdleHeartBeat(queue.getAutoCommitCons(), hearBeatTime);
            longIdleHeartBeat(queue.getManCommitCons(), hearBeatTime);
        }

        //the following is about the idle connection number control
        int idleCons = getIdleCount();
        int totalCount = this.getTotalConCount();
        int createCount = (config.getMinCon() - idleCons) / 3;

        // create if idle too little
        if ((createCount > 0) && totalCount < size) {
            createByIdleLittle(idleCons, createCount);
        } else if (idleCons > config.getMinCon()) {
            closeByIdleMany(idleCons - config.getMinCon(), idleCons);
        }
    }


    /**
     * check if the connection is not be used for a while & do connection heart beat
     *
     * @param linkedQueue
     * @param hearBeatTime
     */
    private void longIdleHeartBeat(ConcurrentLinkedQueue<BackendConnection> linkedQueue, long hearBeatTime) {
        long length = linkedQueue.size();
        for (int i = 0; i < length; i++) {
            BackendConnection con = linkedQueue.poll();
            if (con == null) {
                break;
            } else if (con.isClosed()) {
                continue;
            } else if (con.getLastTime() < hearBeatTime) { //if the connection is idle for a long time
                con.setBorrowed(true);
                new ConnectionHeartBeatHandler().doHeartBeat(con);
            } else {
                linkedQueue.offer(con);
                break;
            }
        }
    }


    private void closeByIdleMany(int idleCloseCount, int idleCons) {
        LOGGER.info("too many ilde cons ,close some for datasouce  " + name + " want close :" + idleCloseCount + " total idle " + idleCons);
        List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(idleCloseCount);
        for (ConQueue queue : conMap.getAllConQueue()) {
            int closeNumber = (queue.getManCommitCons().size() + queue.getAutoCommitCons().size()) * idleCloseCount / idleCons;
            readyCloseCons.addAll(queue.getIdleConsToClose(closeNumber));
        }

        for (BackendConnection idleCon : readyCloseCons) {
            if (idleCon.isBorrowed()) {
                LOGGER.info("find idle con is using " + idleCon);
            }
            idleCon.close("too many idle con");
        }
    }

    private void createByIdleLittle(int idleCons, int createCount) {
        LOGGER.info("create connections ,because idle connection not enough ,cur is " +
                idleCons + ", minCon is " + hostConfig.getMinCon() + " for " + name);

        final String[] schemas = dataHost.getSchemas();
        for (int i = 0; i < createCount; i++) {
            NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();
            try {
                if (!disabled.get() && this.createNewCount()) {
                    // creat new connection
                    this.createNewConnection(simpleHandler, null, schemas[i % schemas.length], false);
                    simpleHandler.getBackConn().release();
                } else {
                    break;
                }
                if (ToResolveContainer.CREATE_CONN_FAIL.contains(this.getHostConfig().getName() + "-" + this.getConfig().getHostName())) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                    AlertUtil.alertResolve(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "mysql", this.getConfig().getId(),
                            labels, ToResolveContainer.CREATE_CONN_FAIL, this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                }
            } catch (IOException e) {
                String errMsg = "create connection err:";
                LOGGER.warn(errMsg, e);
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                AlertUtil.alert(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, errMsg + e.getMessage(), "mysql", this.getConfig().getId(), labels);
                ToResolveContainer.CREATE_CONN_FAIL.add(this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
            }
        }
    }

    public int getTotalConCount() {
        return this.connectionCount.get();
    }

    private boolean createNewCount() {
        int result = this.connectionCount.incrementAndGet();
        if (result > size) {
            this.connectionCount.decrementAndGet();
            return false;
        }
        return true;
    }

    public void clearCons(String reason) {
        this.conMap.clearConnections(reason, this);
    }


    void startHeartbeat() {
        if (!this.isDisabled()) {
            heartbeat.start();
            heartbeat.heartbeat();
        }
    }

    void stopHeartbeat() {
        heartbeat.stop();
    }

    void doHeartbeat() {
        if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
            return;
        }
        heartbeat.heartbeat();
    }

    private BackendConnection takeCon(BackendConnection conn, String schema) {
        conn.setBorrowed(true);

        if (!StringUtil.equals(conn.getSchema(), schema)) {
            // need do schema syn in before sql send
            conn.setSchema(schema);
        }
        if (schema != null) {
            ConQueue queue = conMap.createAndGetSchemaConQueue(schema);
            queue.incExecuteCount();
        }
        // update last time, the schedule job will not close it
        conn.setLastTime(System.currentTimeMillis());
        return conn;
    }

    private void takeCon(BackendConnection conn,
                         final ResponseHandler handler, final Object attachment,
                         String schema) {
        if (ToResolveContainer.CREATE_CONN_FAIL.contains(this.getHostConfig().getName() + "-" + this.getConfig().getHostName())) {
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
            AlertUtil.alertResolve(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "mysql", this.getConfig().getId(), labels,
                    ToResolveContainer.CREATE_CONN_FAIL, this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
        }
        takeCon(conn, schema);
        conn.setAttachment(attachment);
        handler.connectionAcquired(conn);
    }

    private void createNewConnection(final ResponseHandler handler, final Object attachment,
                                     final String schema, final boolean mustWrite) {
        // aysn create connection
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            public void run() {
                try {
                    createNewConnection(new DelegateResponseHandler(handler) {
                        @Override
                        public void connectionError(Throwable e, BackendConnection conn) {
                            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", hostConfig.getName() + "-" + config.getHostName());
                            AlertUtil.alert(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "createNewConn Error" + e.getMessage(), "mysql", config.getId(), labels);
                            ToResolveContainer.CREATE_CONN_FAIL.add(hostConfig.getName() + "-" + config.getHostName());
                            handler.connectionError(e, conn);
                        }

                        @Override
                        public void connectionAcquired(BackendConnection conn) {
                            if (disabled.get()) {
                                handler.connectionError(new IOException("dataSource disabled"), conn);
                                conn.close("disabled dataHost");
                            } else if (mustWrite && isReadNode()) {
                                handler.connectionError(new IOException("writeSource switched"), conn);
                            } else {
                                takeCon(conn, handler, attachment, schema);
                            }
                        }
                    }, schema);
                } catch (IOException e) {
                    handler.connectionError(e, null);
                }
            }
        });
    }

    protected abstract void createNewConnection(ResponseHandler handler, String schema) throws IOException;

    public void getNewConnection(String schema, final ResponseHandler handler,
                                 final Object attachment, boolean mustWrite, boolean forceCreate) throws IOException {
        if (disabled.get()) {
            throw new IOException("the dataSource is disabled [" + this.name + "]");
        } else if (!this.createNewCount()) {
            if (forceCreate) {
                this.connectionCount.incrementAndGet();
                LOGGER.warn("connection pool [" + hostConfig.getName() + "." + this.name + "] has reached maxCon, but we still try to create new connection for important task");
                createNewConnection(handler, attachment, schema, mustWrite);
            } else {
                String maxConError = "the max active Connections size can not be max than maxCon for data host[" + this.getHostConfig().getName() + "." + this.getName() + "]";
                LOGGER.warn(maxConError);
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                AlertUtil.alert(AlarmCode.REACH_MAX_CON, Alert.AlertLevel.WARN, maxConError, "dble", this.getConfig().getId(), labels);
                ToResolveContainer.REACH_MAX_CON.add(this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                throw new IOException(maxConError);
            }
        } else { // create connection
            if (ToResolveContainer.REACH_MAX_CON.contains(this.getHostConfig().getName() + "-" + this.getConfig().getHostName())) {
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                AlertUtil.alertResolve(AlarmCode.REACH_MAX_CON, Alert.AlertLevel.WARN, "dble", this.getConfig().getId(), labels,
                        ToResolveContainer.REACH_MAX_CON, this.getHostConfig().getName() + "-" + this.getConfig().getHostName());

            }
            LOGGER.info("no idle connection in pool [" + hostConfig.getName() + "." + this.name + "],create new connection for  schema: " + schema);
            createNewConnection(handler, attachment, schema, mustWrite);
        }
    }

    public void getConnection(String schema, boolean autocommit, final ResponseHandler handler,
                              final Object attachment, boolean mustWrite) throws IOException {
        BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con != null) {
            takeCon(con, handler, attachment, schema);
        } else {
            getNewConnection(schema, handler, attachment, mustWrite, false);
        }
    }


    public BackendConnection getConnection(String schema, boolean autocommit, final Object attachment) throws IOException {
        BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con == null) {
            if (disabled.get()) {
                throw new IOException("the dataSource is disabled [" + this.name + "]");
            } else if (!this.createNewCount()) {
                String maxConError = "the max active Connections size can not be max than maxCon data host[" + this.getHostConfig().getName() + "." + this.getName() + "]";
                LOGGER.warn(maxConError);
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                AlertUtil.alert(AlarmCode.REACH_MAX_CON, Alert.AlertLevel.WARN, maxConError, "dble", this.getConfig().getId(), labels);
                ToResolveContainer.REACH_MAX_CON.add(this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                throw new IOException(maxConError);
            } else { // create connection
                if (ToResolveContainer.REACH_MAX_CON.contains(this.getHostConfig().getName() + "-" + this.getConfig().getHostName())) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                    AlertUtil.alertResolve(AlarmCode.REACH_MAX_CON, Alert.AlertLevel.WARN, "dble", this.getConfig().getId(), labels,
                            ToResolveContainer.REACH_MAX_CON, this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                }
                LOGGER.info("no ilde connection in pool,create new connection for " + this.name + " of schema " + schema);
                con = createNewBackendConnection(schema);
            }
        }
        con = takeCon(con, schema);
        con.setAttachment(attachment);
        return con;
    }

    public BackendConnection getConnectionForHeartbeat(String schema) throws IOException {
        BackendConnection con;
        if (!disabled.get()) {
            if (!this.createNewCount()) {
                ConQueue queue = conMap.getSchemaConQueue(null);
                BackendConnection conIdle = queue.takeIdleCon(true);
                this.connectionCount.incrementAndGet();
                if (conIdle != null) {
                    conIdle.close("create new connection for heartbeat, so close an old idle con");
                } else {
                    LOGGER.warn("now connection in pool and reached maxCon, but still try to create new connection for heartbeat ");
                }
                con = createNewBackendConnection(schema);
            } else { // create connection
                LOGGER.info("create new connection for heartbeat ");
                con = createNewBackendConnection(schema);
            }
        } else {
            return null;
        }
        con = takeCon(con, schema);
        con.setAttachment(null);
        return con;
    }

    private BackendConnection createNewBackendConnection(String schema) throws IOException {
        BackendConnection con;
        try {
            NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();
            this.createNewConnection(simpleHandler, schema);
            con = simpleHandler.getBackConn();
            if (ToResolveContainer.CREATE_CONN_FAIL.contains(this.getHostConfig().getName() + "-" + this.getConfig().getHostName())) {
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
                AlertUtil.alertResolve(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "mysql", this.getConfig().getId(), labels,
                        ToResolveContainer.CREATE_CONN_FAIL, this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
            }
        } catch (IOException e) {
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
            AlertUtil.alert(AlarmCode.CREATE_CONN_FAIL, Alert.AlertLevel.WARN, "createNewConn Error" + e.getMessage(), "mysql", this.getConfig().getId(), labels);
            ToResolveContainer.CREATE_CONN_FAIL.add(this.getHostConfig().getName() + "-" + this.getConfig().getHostName());
            throw e;
        }
        return con;
    }

    void initMinConnection(String schema, boolean autocommit, final ResponseHandler handler,
                           final Object attachment) throws IOException {
        LOGGER.info("create new connection for " +
                this.name + " of schema " + schema);
        if (this.createNewCount()) {
            createNewConnection(handler, attachment, schema, false);
        }
    }

    private void returnCon(BackendConnection c) {
        if (c.isClosed()) {
            return;
        }

        c.setAttachment(null);
        c.setBorrowed(false);
        c.setLastTime(TimeUtil.currentTimeMillis());

        String errMsg = null;

        boolean ok;
        ConQueue queue = this.conMap.createAndGetSchemaConQueue(c.getSchema());
        if (c.isAutocommit()) {
            ok = queue.getAutoCommitCons().offer(c);
        } else {
            ok = queue.getManCommitCons().offer(c);
        }
        if (!ok) {
            errMsg = "can't return to pool ,so close con " + c;
        }
        if (errMsg != null) {
            LOGGER.info(errMsg);
            c.close(errMsg);
        }
    }

    public void releaseChannel(BackendConnection c) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("release channel " + c);
        }
        // release connection
        returnCon(c);
    }

    public void connectionClosed(BackendConnection conn) {
        //only used in mysqlConneciton synchronized function
        this.connectionCount.decrementAndGet();
        ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
        if (queue != null) {
            queue.removeCon(conn);
        }
    }

    /**
     * used for init or reload
     */
    public abstract boolean testConnection() throws IOException;

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

    public DataSourceConfig getConfig() {
        return config;
    }

    public boolean isAlive() {
        return !disabled.get() && ((heartbeat.getStatus() == MySQLHeartbeat.INIT_STATUS && testConnSuccess) || heartbeat.isHeartBeatOK());
    }


    public boolean equals(PhysicalDataSource dataSource) {
        return dataSource.getConfig().getUser().equals(this.getConfig().getUser()) && dataSource.getConfig().getUrl().equals(this.getConfig().getUrl()) &&
                dataSource.getConfig().getPassword().equals(this.getConfig().getPassword()) && dataSource.getConfig().getHostName().equals(this.getConfig().getHostName()) &&
                dataSource.isDisabled() == this.isDisabled() && dataSource.getConfig().getWeight() == this.getConfig().getWeight();
    }

    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public int hashCode() {
        return super.hashCode();
    }

    public boolean isDisabled() {
        return disabled.get();
    }

    boolean setDisabled(boolean value) {
        if (value) {
            return disabled.compareAndSet(false, true);
        } else {
            return disabled.compareAndSet(true, false);
        }
    }

    @Override
    public String toString() {
        return "dataSource[name=" + name +
                ",disabled=" +
                disabled.toString() + ",maxCon=" +
                size + "]";
    }

}
