package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.ServerSptPrepare;
import com.actiontech.dble.server.handler.ServerLoadDataInfileHandler;
import com.actiontech.dble.server.handler.ServerPrepareHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.server.variables.VariableType;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlsharding.handler.LoadDataProtoHandlerImpl;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.SerializableLock;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by szf on 2020/6/18.
 */
public class ShardingService extends BusinessService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);

    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<>();

    private final ServerQueryHandler handler;

    private final ServerLoadDataInfileHandler loadDataInfileHandler;

    private final FrontendPrepareHandler prepareHandler;

    private final MySQLProtoLogicHandler protoLogicHandler;

    private final MySQLShardingSQLHandler shardingSQLHandler;

    protected String executeSql;
    private volatile boolean txChainBegin;
    private volatile boolean txInterrupted;
    private volatile String txInterruptMsg = "";

    protected long lastReadTime;
    private AtomicLong txID = new AtomicLong(1);
    private volatile boolean isLocked = false;
    private long lastInsertId;
    private volatile boolean multiStatementAllow = false;
    private final NonBlockingSession session;
    private boolean sessionReadOnly = false;
    private ServerSptPrepare sptprepare;

    public ShardingService(AbstractConnection connection) {
        super(connection);
        this.sptprepare = new ServerSptPrepare(this);
        this.handler = new ServerQueryHandler(this);
        this.loadDataInfileHandler = new ServerLoadDataInfileHandler(this);
        this.prepareHandler = new ServerPrepareHandler(this);
        this.session = new NonBlockingSession(this);
        session.setRowCount(0);
        this.protoLogicHandler = new MySQLProtoLogicHandler(this);
        this.shardingSQLHandler = new MySQLShardingSQLHandler(this);
        this.proto = new MySQLProtoHandlerImpl();
    }

    @Override
    public void handleVariable(MysqlVariable var) {
        String val = var.getValue();
        switch (var.getType()) {
            case XA:
                session.getTransactionManager().setXaTxEnabled(Boolean.parseBoolean(val), this);
                this.singleTransactionsCount();
                break;
            case TRACE:
                session.setTrace(Boolean.parseBoolean(val));
                this.singleTransactionsCount();
                break;
            case TX_READ_ONLY:
                sessionReadOnly = Boolean.parseBoolean(val);
                this.singleTransactionsCount();
                break;
            case AUTOCOMMIT:
                if (Boolean.parseBoolean(val)) {
                    if (!autocommit && session.getTargetCount() > 0) {
                        session.implicitCommit(() -> {
                            autocommit = true;
                            txStarted = false;
                            this.singleTransactionsCount();
                            writeOkPacket();
                        });
                        return;
                    }
                    autocommit = true;
                } else {
                    if (autocommit) {
                        autocommit = false;
                        txStarted = true;
                        TxnLogHelper.putTxnLog(this, executeSql);
                    }
                }
                this.singleTransactionsCount();
                writeOkPacket();
                break;
            default:
                // IGNORE
        }
    }

    @Override
    public List<MysqlVariable> getAllVars() {
        List<MysqlVariable> variables = super.getAllVars();
        variables.add(new MysqlVariable("xa", session.getTransactionManager().getSessionXaID() == null ? "false" : "true", VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("trace", session.isTrace() + "", VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable(VersionUtil.TRANSACTION_READ_ONLY, sessionReadOnly + "", VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable(VersionUtil.TX_READ_ONLY, sessionReadOnly + "", VariableType.SYSTEM_VARIABLES));
        return variables;
    }

    public void checkXaStatus(boolean val) throws SQLSyntaxErrorException {
        if (val) {
            if (session.getTargetMap().size() > 0 && session.getSessionXaID() == null) {
                throw new SQLSyntaxErrorException("you can't set xa cmd on when there are unfinished operation in the session.");
            }
        } else {
            if (session.getTargetMap().size() > 0 && session.getSessionXaID() != null) {
                throw new SQLSyntaxErrorException("you can't set xa cmd off when a transaction is in progress.");
            }
        }
    }

    public void query(String sql) {
        sql = sql.trim();
        // remove last ';'
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        WallProvider blackList = ((ShardingUserConfig) userConfig).getBlacklist();
        if (blackList != null) {
            WallCheckResult result = blackList.check(sql);
            if (!result.getViolations().isEmpty()) {
                LOGGER.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:" +
                        result.getViolations().get(0).getMessage() + " \r\n " + sql);
                writeErrMessage(ErrorCode.ERR_WRONG_USED, "The statement is unsafe SQL, reject for user '" + user + "'");
                return;
            }
        }

        SerializableLock.getInstance().lock(this.connection.getId());

        boolean readOnly = false;
        if (userConfig instanceof ShardingUserConfig) {
            readOnly = ((ShardingUserConfig) userConfig).isReadOnly();
        }

        this.handler.setReadOnly(readOnly);
        this.handler.setSessionReadOnly(sessionReadOnly);
        this.handler.query(sql);
    }

    @Override
    protected void taskToTotalQueue(ServiceTask task) {
        session.setRequestTime();
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        getSession2().startProcess();
        /*if (isAuthSwitch.compareAndSet(true, false)) {
            commands.doOther();
            sc.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }*/
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                protoLogicHandler.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                protoLogicHandler.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                Ping.response(connection);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                Heartbeat.response(connection, data);
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                connection.close("quit cmd");
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                String prepareSql = protoLogicHandler.stmtPrepare(data);
                // record SQL
                if (prepareSql != null) {
                    this.setExecuteSql(prepareSql);
                    prepareHandler.prepare(prepareSql);
                }
                break;
            case MySQLPacket.COM_STMT_SEND_LONG_DATA:
                commands.doStmtSendLongData();
                blobDataQueue.offer(data);
                break;
            case MySQLPacket.COM_STMT_CLOSE:
                commands.doStmtClose();
                stmtClose(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                blobDataQueue.clear();
                prepareHandler.reset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                this.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                protoLogicHandler.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                commands.doOther();
                /* changeUserPacket = new ChangeUserPacket(sc.getClientFlags(), CharsetUtil.getCollationIndex(sc.getCharset().getCollation()));
                sc.changeUser(data, changeUserPacket, isAuthSwitch);*/
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                protoLogicHandler.resetConnection();
                break;
            case MySQLPacket.COM_FIELD_LIST:
                commands.doOther();
                protoLogicHandler.fieldList(data);
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                break;
            default:
                commands.doOther();
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }


    public void execute(String sql, int type) {
        try {
            if (connection.isClosed()) {
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
            shardingSQLHandler.routeEndExecuteSQL(sql, type, schemaConfig);

        } catch (Exception e) {
            writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        }
    }


    public void stmtExecute(byte[] data, Queue<byte[]> dataQueue) {
        byte[] sendData = dataQueue.poll();
        while (sendData != null) {
            this.stmtSendLongData(sendData);
            sendData = dataQueue.poll();
        }
        if (prepareHandler != null) {
            prepareHandler.execute(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void stmtSendLongData(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.sendLongData(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void stmtClose(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.close(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void setTxInterrupt(String msg) {
        if ((!autocommit || txStarted) && !txInterrupted) {
            txInterrupted = true;
            this.txInterruptMsg = "Transaction error, need to rollback.Reason:[" + msg + "]";
        }
    }


    public void innerCleanUp() {
        //rollback and unlock tables  means close backend conns;
        Iterator<BackendConnection> connIterator = session.getTargetMap().values().iterator();
        while (connIterator.hasNext()) {
            BackendConnection conn = connIterator.next();
            conn.businessClose("com_reset_connection");
            connIterator.remove();
        }

        isLocked = false;
        txChainBegin = false;
        txStarted = false;
        txInterrupted = false;

        this.sysVariables.clear();
        this.usrVariables.clear();
        autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        txIsolation = SystemConfig.getInstance().getTxIsolation();
        setCharacterSet(SystemConfig.getInstance().getCharset());

        lastInsertId = 0;
        //prepare
        if (prepareHandler != null) {
            prepareHandler.clear();
        }
    }

    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        this.shardingSQLHandler.routeSystemInfoAndExecuteSQL(stmt, schemaInfo, sqlType);
    }

    @Override
    public void initFromAuthInfo(AuthResultInfo info) {
        super.initFromAuthInfo(info);
        this.multiStatementAllow = info.getMysqlAuthPacket().isMultStatementAllow();
    }

    public void writeErrMessage(String sqlState, String msg, int vendorCode) {
        byte packetId = (byte) this.getSession2().getPacketId().get();
        writeErrMessage(++packetId, vendorCode, sqlState, msg);
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
    }

    @Override
    protected void writeErrMessage(byte id, int vendorCode, String sqlState, String msg) {
        markFinished();
        super.writeErrMessage(id, vendorCode, sqlState, msg);
    }

    public void markFinished() {
        if (session != null) {
            session.setStageFinished();
        }
    }

    public void beginInTx(String stmt) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            TxnLogHelper.putTxnLog(session.getShardingService(), "commit[because of " + stmt + "]");
            this.txChainBegin = true;
            session.commit();
            txStarted = true;
            TxnLogHelper.putTxnLog(session.getShardingService(), stmt);
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


    public void commit(String logReason) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            TxnLogHelper.putTxnLog(session.getShardingService(), logReason);
            session.commit();
        }
    }

    public void rollback() {
        if (txInterrupted) {
            txInterrupted = false;
        }

        session.rollback();
    }

    public void lockTable(String sql) {
        if ((!isAutocommit() || isTxStart())) {
            session.implicitCommit(() -> doLockTable(sql));
            return;
        }
        doLockTable(sql);
    }

    public void unLockTable(String sql) {
        sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
        String[] words = SplitUtil.split(sql, ' ', true);
        if (words.length == 2 && ("table".equalsIgnoreCase(words[1]) || "tables".equalsIgnoreCase(words[1]))) {
            isLocked = false;
            session.unLockTable(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void loadDataInfileStart(String sql) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.clear();
                proto = new LoadDataProtoHandlerImpl(loadDataInfileHandler);
                loadDataInfileHandler.start(sql);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }

        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile sql is not  unsupported!");
        }
    }

    private void doLockTable(String sql) {
        String db = this.schema;
        SchemaConfig schemaConfig = null;
        if (this.schema != null) {
            schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(this.schema);
            if (schemaConfig == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
                return;
            }
        }

        RouteResultset rrs;
        try {
            rrs = RouteService.getInstance().route(schemaConfig, ServerParse.LOCK, sql, this);
        } catch (Exception e) {
            executeException(e, sql);
            return;
        }

        if (rrs != null) {
            session.lockTable(rrs);
        }
    }

    private void executeException(Exception e, String sql) {
        sql = sql.length() > 1024 ? sql.substring(0, 1024) + "..." : sql;
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

    @Override
    public void write(MySQLPacket packet) {
        boolean multiQueryFlag = session.multiStatementPacket(packet);
        if (packet.isEndOfSession()) {
            //error finished do resource clean up
            session.resetMultiStatementStatus();
            for (BackendConnection backendConnection : session.getTargetMap().values()) {
                TraceManager.sessionFinish(backendConnection.getBackendService());
            }
            TraceManager.sessionFinish(this);
            packet.bufferWrite(connection);
            SerializableLock.getInstance().unLock(this.connection.getId());
        } else if (packet.isEndOfQuery()) {
            //normal finish may loop to another round of query
            packet.bufferWrite(connection);
            for (BackendConnection backendConnection : session.getTargetMap().values()) {
                TraceManager.sessionFinish(backendConnection.getBackendService());
            }
            if (multiQueryFlag) {
                TraceManager.queryFinish(this);
            } else {
                TraceManager.sessionFinish(this);
            }
            multiStatementNextSql(multiQueryFlag);
            SerializableLock.getInstance().unLock(this.connection.getId());
        } else {
            packet.bufferWrite(connection);
        }
    }

    @Override
    public void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        boolean multiQueryFlag = session.multiStatementPacket(packet);
        if (packet.isEndOfSession()) {
            //error finished do resource clean up
            session.resetMultiStatementStatus();
            for (BackendConnection backendConnection : session.getTargetMap().values()) {
                TraceManager.sessionFinish(backendConnection.getBackendService());
            }
            TraceManager.sessionFinish(this);
        }
        buffer = packet.write(buffer, this, true);
        connection.write(buffer);
        if (packet.isEndOfQuery() && !packet.isEndOfSession()) {
            for (BackendConnection backendConnection : session.getTargetMap().values()) {
                TraceManager.sessionFinish(backendConnection.getBackendService());
            }
            if (multiQueryFlag) {
                TraceManager.queryFinish(this);
            } else {
                TraceManager.sessionFinish(this);
            }
            multiStatementNextSql(multiQueryFlag);
        }
        SerializableLock.getInstance().unLock(this.connection.getId());
    }


    public void multiStatementNextSql(boolean flag) {
        if (flag) {
            session.setRequestTime();
            session.setQueryStartTime(System.currentTimeMillis());
            taskMultiQueryCreate(protoLogicHandler.getMultiQueryData());
        }
    }


    public void cleanup() {
        super.cleanup();
        if (session != null) {
            TsQueriesCounter.getInstance().addToHistory(this);
            session.terminate();
        }
        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
    }

    protected void sessionStart() {
        TraceManager.sessionStart(this, "sharding-server-start");
    }

    public boolean isTxChainBegin() {
        return txChainBegin;
    }

    public void setTxChainBegin(boolean txChainBegin) {
        this.txChainBegin = txChainBegin;
    }

    public boolean isTxInterrupted() {
        return txInterrupted;
    }

    public void setTxInterrupted(boolean txInterrupted) {
        this.txInterrupted = txInterrupted;
    }

    public String getTxInterruptMsg() {
        return txInterruptMsg;
    }

    public void setTxInterruptMsg(String txInterruptMsg) {
        this.txInterruptMsg = txInterruptMsg;
    }

    public String getExecuteSql() {
        return executeSql;
    }

    @Override
    public void killAndClose(String reason) {
        connection.close(reason);
        if (!isTxStart() || session.getTransactionManager().getXAStage() == null) {
            //not a xa transaction ,close it
            session.kill();
        }
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
    }

    public boolean isMultiStatementAllow() {
        return multiStatementAllow;
    }

    public void setMultiStatementAllow(boolean multiStatementAllow) {
        this.multiStatementAllow = multiStatementAllow;
    }

    public NonBlockingSession getSession2() {
        return session;
    }

    public ShardingUserConfig getUserConfig() {
        return (ShardingUserConfig) userConfig;
    }

    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public long getAndIncrementXid() {
        return txID.getAndIncrement();
    }

    public long getXid() {
        return txID.get();
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    public void updateLastReadTime() {
        this.lastReadTime = TimeUtil.currentTimeMillis();
    }

    public void setSessionReadOnly(boolean sessionReadOnly) {
        this.sessionReadOnly = sessionReadOnly;
    }

    public boolean isReadOnly() {
        return sessionReadOnly;
    }

    public ServerLoadDataInfileHandler getLoadDataInfileHandler() {
        return loadDataInfileHandler;
    }

    public ServerSptPrepare getSptPrepare() {
        return sptprepare;
    }

    public void resetProto() {
        this.proto = new MySQLProtoHandlerImpl();
    }

    public String toString() {
        return "Shardingservice[ user = " + user + " schema = " + schema + " executeSql = " + executeSql + " txInterruptMsg = " + txInterruptMsg +
                " sessionReadOnly = " + sessionReadOnly + "] with connection " + connection.toString() + " with session " + session.toString();
    }
}
