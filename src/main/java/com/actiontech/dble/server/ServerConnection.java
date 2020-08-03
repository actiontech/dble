/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLTraceInfo;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.SetInnerHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.server.response.ShowCreateView;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.singleton.*;
import com.actiontech.dble.util.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private volatile int txIsolation;
    private volatile boolean autocommit;
    private volatile boolean txStarted;
    private volatile boolean txChainBegin;
    private volatile boolean txInterrupted;
    private volatile String txInterruptMsg = "";
    private ServerSptPrepare sptprepare;
    private long lastInsertId;
    private NonBlockingSession session;
    private volatile boolean isLocked = false;
    private AtomicLong txID;
    private List<Pair<SetHandler.KeyType, Pair<String, String>>> contextTask = new ArrayList<>();
    private List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();

    public ServerConnection(NetworkChannel channel)
            throws IOException {
        super(channel);
        this.txInterrupted = false;
        this.autocommit = DbleServer.getInstance().getConfig().getSystem().getAutocommit() == 1;
        this.txID = new AtomicLong(1);
        this.sptprepare = new ServerSptPrepare(this);
        this.usrVariables = new LinkedHashMap<>();
        this.sysVariables = new LinkedHashMap<>();
    }


    public ServerConnection() {
        /* just for unit test */
    }

    public long getAndIncrementXid() {
        return txID.getAndIncrement();
    }

    public long getXid() {
        return txID.get();
    }

    public ServerSptPrepare getSptPrepare() {
        return sptprepare;
    }

    public void setSptprepare(ServerSptPrepare sptprepare) {
        this.sptprepare = sptprepare;
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
                    lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    public boolean isTxStart() {
        return txStarted;
    }

    public void setTxStart(boolean txStart) {
        if (!txStart && txChainBegin) {
            txChainBegin = false;
        } else {
            this.txStarted = txStart;
        }
    }

    public void setTxInterrupt(String msg) {
        if ((!autocommit || txStarted) && !txInterrupted) {
            txInterrupted = true;
            this.txInterruptMsg = "Transaction error, need to rollback.Reason:[" + msg + "]";
        }
    }

    public NonBlockingSession getSession2() {
        return session;
    }

    void setSession2(NonBlockingSession session2) {
        this.session = session2;
    }

    public boolean isLocked() {
        return isLocked;
    }

    void setLocked(boolean locked) {
        this.isLocked = locked;
    }


    public List<Pair<SetHandler.KeyType, Pair<String, String>>> getContextTask() {
        return contextTask;
    }

    public void setContextTask(List<Pair<SetHandler.KeyType, Pair<String, String>>> contextTask) {
        this.contextTask = contextTask;
    }


    public List<Pair<SetHandler.KeyType, Pair<String, String>>> getInnerSetTask() {
        return innerSetTask;
    }

    public void setInnerSetTask(List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask) {
        this.innerSetTask = innerSetTask;
    }

    @Override
    protected void setRequestTime() {
        session.setRequestTime();

    }

    @Override
    public void startProcess() {
        session.startProcess();
    }

    @Override
    public void markFinished() {
        session.setStageFinished();
    }

    public boolean executeInnerSetTask() {
        Pair<SetHandler.KeyType, Pair<String, String>> autoCommitTask = null;
        for (Pair<SetHandler.KeyType, Pair<String, String>> task : innerSetTask) {
            switch (task.getKey()) {
                case XA:
                    session.getTransactionManager().setXaTxEnabled(Boolean.valueOf(task.getValue().getKey()), this);
                    break;
                case AUTOCOMMIT:
                    autoCommitTask = task;
                    break;
                case TRACE:
                    session.setTrace(Boolean.valueOf(task.getValue().getKey()));
                    break;
                default:
            }
        }

        if (autoCommitTask != null) {
            return SetInnerHandler.execSetAutoCommit(executeSql, this, Boolean.valueOf(autoCommitTask.getValue().getKey()));
        }
        return false;
    }

    public void executeContextSetTask() {
        for (Pair<SetHandler.KeyType, Pair<String, String>> task : contextTask) {
            switch (task.getKey()) {
                case CHARACTER_SET_CLIENT:
                    String charsetClient = task.getValue().getKey();
                    this.setCharacterClient(charsetClient);
                    break;
                case CHARACTER_SET_CONNECTION:
                    String collationName = task.getValue().getKey();
                    this.setCharacterConnection(collationName);
                    break;
                case CHARACTER_SET_RESULTS:
                    String charsetResult = task.getValue().getKey();
                    this.setCharacterResults(charsetResult);
                    break;
                case COLLATION_CONNECTION:
                    String collation = task.getValue().getKey();
                    this.setCollationConnection(collation);
                    break;
                case TX_ISOLATION:
                    String isolationLevel = task.getValue().getKey();
                    this.setTxIsolation(Integer.parseInt(isolationLevel));
                    break;
                case TX_READ_ONLY:
                    String enable = task.getValue().getKey();
                    this.setSessionReadOnly(Boolean.parseBoolean(enable));
                    break;
                case SYSTEM_VARIABLES:
                    this.sysVariables.put(task.getValue().getKey(), task.getValue().getValue());
                    break;
                case USER_VARIABLES:
                    this.usrVariables.put(task.getValue().getKey(), task.getValue().getValue());
                    break;
                case CHARSET:
                    this.setCharacterSet(task.getValue().getKey());
                    break;
                case NAMES:
                    this.setNames(task.getValue().getKey(), task.getValue().getValue());
                    break;
                default:
                    //can't happen
                    break;
            }
        }
    }

    @Override
    public void ping() {
        Ping.response(this);
    }

    @Override
    public void heartbeat(byte[] data) {
        Heartbeat.response(this, data);
    }

    public void execute(String sql, int type) {
        if (this.isClosed()) {
            LOGGER.info("ignore execute ,server connection is closed " + this);
            return;
        }
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
            return;
        }
        session.setQueryStartTime(System.currentTimeMillis());

        String db = this.schema;

        SchemaConfig schemaConfig = null;
        if (db != null) {
            schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(db);
            if (schemaConfig == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
                return;
            }
        }
        //fix navicat
        // SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `Percentage`
        // FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ
        if (ServerParse.SELECT == type && sql.contains(" INFORMATION_SCHEMA.PROFILING ") && sql.contains("CONCAT(ROUND(SUM(DURATION)/")) {
            InformationSchemaProfiling.response(this);
            return;
        }
        routeEndExecuteSQL(sql, type, schemaConfig);

    }

    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig user = conf.getUsers().get(this.getUser());
        if (user == null || !user.getSchemas().contains(schemaInfo.getSchema())) {
            writeErrMessage("42000", "Access denied for user '" + this.getUser() + "' to database '" + schemaInfo.getSchema() + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        RouteResultset rrs = new RouteResultset(stmt, sqlType);
        try {
            String noShardingNode = RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            if (noShardingNode != null) {
                RouterUtil.routeToSingleNode(rrs, noShardingNode);
            } else {
                TableConfig tc = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
                if (tc == null) {
                    // check view
                    ShowCreateView.response(this, schemaInfo.getSchema(), schemaInfo.getTable());
                    return;
                }
                RouterUtil.routeToRandomNode(rrs, schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            }
            session.execute(rrs);
        } catch (Exception e) {
            executeException(e, stmt);
        }
    }

    private void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
        RouteResultset rrs = null;
        try {
            if (session.isKilled()) {
                writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
                return;
            }

            rrs = RouteService.getInstance().route(schema, type, sql, this);
            if (rrs == null) {
                return;
            }
            if (rrs.getSqlType() == ServerParse.DDL && rrs.getSchema() != null) {
                DDLTraceManager.getInstance().startDDL(this);
                addTableMetaLock(rrs);
                if (ProxyMeta.getInstance().getTmManager().getCatalogs().get(rrs.getSchema()).getView(rrs.getTable()) != null) {
                    ProxyMeta.getInstance().getTmManager().removeMetaLock(rrs.getSchema(), rrs.getTable());
                    String msg = "Table '" + rrs.getTable() + "' already exists as a view";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
                DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.LOCK_END, this);
            }
        } catch (Exception e) {
            if (rrs != null && rrs.getSqlType() == ServerParse.DDL && rrs.getSchema() != null) {
                DDLTraceManager.getInstance().endDDL(this, e.getMessage());
            }
            executeException(e, sql);
            return;
        }
        session.endRoute(rrs);
        session.execute(rrs);
    }

    private void addTableMetaLock(RouteResultset rrs) throws SQLNonTransientException {
        String schema = rrs.getSchema();
        String table = rrs.getTable();
        try {
            //lock self meta
            ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, rrs.getSrcStatement());
            if (ClusterGeneralConfig.isUseZK()) {
                String nodeName = StringUtil.getFullName(schema, table);
                String ddlPath = KVPathUtil.getDDLPath();
                String nodePth = ZKPaths.makePath(ddlPath, nodeName);
                CuratorFramework zkConn = ZKUtils.getConnection();
                if (zkConn.checkExists().forPath(KVPathUtil.getSyncMetaLockPath()) != null || zkConn.checkExists().forPath(nodePth) != null) {
                    String msg = "The metaLock about `" + nodeName + "` is exists. It means other instance is doing DDL.";
                    LOGGER.info(msg + " The path of DDL is " + ddlPath);
                    throw new Exception(msg);
                }
                ProxyMeta.getInstance().getTmManager().notifyClusterDDL(schema, table, rrs.getStatement());
            } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
                ProxyMeta.getInstance().getTmManager().notifyClusterDDL(schema, table, rrs.getStatement());
            }
        } catch (SQLNonTransientException e) {
            throw e;
        } catch (Exception e) {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
            throw new SQLNonTransientException(e.toString() + ",sql:" + rrs.getStatement());
        }
    }

    private void executeException(Exception e, String sql) {
        if (e instanceof SQLException) {
            SQLException sqlException = (SQLException) e;
            String msg = sqlException.getMessage();
            StringBuilder s = new StringBuilder();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(s.append(this).append(sql).toString() + " err:" + msg);
            }
            int vendorCode = sqlException.getErrorCode() == 0 ? ErrorCode.ER_PARSE_ERROR : sqlException.getErrorCode();
            String sqlState = StringUtil.isEmpty(sqlException.getSQLState()) ? "HY000" : sqlException.getSQLState();
            String errorMsg = msg == null ? sqlException.getClass().getSimpleName() : msg;
            writeErrMessage(sqlState, errorMsg, vendorCode);
        } else {
            StringBuilder s = new StringBuilder();
            LOGGER.info(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
        }
    }

    /**
     * begin without commit means commit and begin
     */
    public void beginInTx(String stmt) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            TxnLogHelper.putTxnLog(this, "commit[because of " + stmt + "]");
            this.txChainBegin = true;
            session.commit();
            TxnLogHelper.putTxnLog(this, stmt);
        }
    }

    public void commit(String logReason) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            TxnLogHelper.putTxnLog(this, logReason);
            session.commit();
        }
    }

    // savepoint
    public void performSavePoint(String spName, SavePointHandler.Type type) {
        if (!autocommit || isTxStart()) {
            if (type == SavePointHandler.Type.ROLLBACK && txInterrupted) {
                txInterrupted = false;
            }
            session.performSavePoint(spName, type);
        } else {
            writeErrMessage(ErrorCode.ER_YES, "please use in transaction!");
        }
    }

    public void rollback() {
        if (txInterrupted) {
            txInterrupted = false;
        }

        session.rollback();
    }

    void lockTable(String sql) {
        if ((!isAutocommit() || isTxStart())) {
            session.implictCommit(() -> doLockTable(sql));
            return;
        }
        doLockTable(sql);
    }

    private void doLockTable(String sql) {
        String db = this.schema;
        SchemaConfig schema = null;
        if (this.schema != null) {
            schema = DbleServer.getInstance().getConfig().getSchemas().get(this.schema);
            if (schema == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
                return;
            }
        }

        RouteResultset rrs;
        try {
            rrs = RouteService.getInstance().route(schema, ServerParse.LOCK, sql, this);
        } catch (Exception e) {
            executeException(e, sql);
            return;
        }

        if (rrs != null) {
            session.lockTable(rrs);
        }
    }

    void unLockTable(String sql) {
        sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
        String[] words = SplitUtil.split(sql, ' ', true);
        if (words.length == 2 && ("table".equalsIgnoreCase(words[1]) || "tables".equalsIgnoreCase(words[1]))) {
            isLocked = false;
            session.unLockTable(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void innerCleanUp() {
        //rollback and unlock tables  means close backend conns;
        Iterator<BackendConnection> connIterator = session.getTargetMap().values().iterator();
        while (connIterator.hasNext()) {
            BackendConnection conn = connIterator.next();
            conn.closeWithoutRsp("com_reset_connection");
            connIterator.remove();
        }
        isLocked = false;
        txChainBegin = false;
        txStarted = false;
        txInterrupted = false;

        this.getSysVariables().clear();
        this.getUsrVariables().clear();
        autocommit = DbleServer.getInstance().getConfig().getSystem().getAutocommit() == 1;
        txIsolation = DbleServer.getInstance().getConfig().getSystem().getTxIsolation();
        this.setCharacterSet(DbleServer.getInstance().getConfig().getSystem().getCharset());
        lastInsertId = 0;

        //prepare
        if (prepareHandler != null) {
            prepareHandler.clear();
        }
    }

    @Override
    public synchronized void close(String reason) {

        TsQueriesCounter.getInstance().addToHistory(session);
        //XA transaction in this phase,close it
        if (session.getSource().isTxStart() && session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_CANCELING) &&
                session.getTransactionManager().getXAStage() != null) {
            super.close(reason);
            session.initiativeTerminate();
        } else if (session.getSource().isTxStart() &&
                session.getTransactionManager().getXAStage() != null) {
            //XA transaction in this phase(commit/rollback) close the front end and wait for the backend finished
            super.close(reason);
        } else {
            //not a xa transaction ,close it
            super.close(reason);
            session.terminate();
        }

        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
    }

    @Override
    public void killAndClose(String reason) {
        if (session.getSource().isTxStart() && !session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_CANCELING) &&
                session.getTransactionManager().getXAStage() != null) {
            //XA transaction in this phase(commit/rollback) close the front end and wait for the backend finished
            super.close(reason);
        } else {
            //not a xa transaction ,close it
            super.close(reason);
            session.kill();
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("ServerConnection [frontId=");
        result.append(id);
        result.append(", schema=");
        result.append(schema);
        result.append(", host=");
        result.append(host);
        result.append(", user=");
        result.append(user);
        result.append(",txIsolation=");
        result.append(txIsolation);
        result.append(", autocommit=");
        result.append(autocommit);
        result.append(", schema=");
        result.append(schema);
        if (sysVariables.size() > 0) {
            result.append(", ");
            result.append(getStringOfSysVariables());
        }
        if (usrVariables.size() > 0) {
            result.append(", ");
            result.append(getStringOfUsrVariables());
        }
        result.append("]");
        return result.toString();
    }

    @Override
    public void writeErrMessage(String sqlState, String msg, int vendorCode) {
        byte packetId = (byte) this.getSession2().getPacketId().get();
        super.writeErrMessage(++packetId, vendorCode, sqlState, msg);
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
    }

    @Override
    public void writeErrMessage(int vendorCode, String msg) {
        byte packetId = (byte) this.getSession2().getPacketId().get();
        super.writeErrMessage(++packetId, vendorCode, msg);
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
    }

    @Override
    public void write(byte[] data) {
        SerializableLock.getInstance().unLock(this.id);
        markFinished();
        super.write(data);
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
    }

    @Override
    public final void write(ByteBuffer buffer) {
        SerializableLock.getInstance().unLock(this.id);
        markFinished();
        super.write(buffer);
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
    }

    @Override
    public void writePart(ByteBuffer buffer) {
        super.write(buffer);
    }

    @Override
    public void stopFlowControl() {
        session.stopFlowControl();
    }

    public void startFlowControl(BackendConnection backendConnection) {
        session.startFlowControl(backendConnection);
    }
}
