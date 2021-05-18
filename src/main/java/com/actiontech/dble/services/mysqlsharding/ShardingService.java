package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.log.general.GeneralLogHelper;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.RequestScope;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.ServerSptPrepare;
import com.actiontech.dble.server.handler.ServerLoadDataInfileHandler;
import com.actiontech.dble.server.handler.ServerPrepareHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.server.variables.VariableType;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLChangeUserService;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.SerializableLock;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by szf on 2020/6/18.
 */
public class ShardingService extends BusinessService<ShardingUserConfig> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);

    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<>();

    private final ServerQueryHandler handler;
    private final ServerLoadDataInfileHandler loadDataInfileHandler;
    private final FrontendPrepareHandler prepareHandler;
    private final MySQLProtoLogicHandler protoLogicHandler;
    private final MySQLShardingSQLHandler shardingSQLHandler;

    private volatile boolean txInterrupted;
    private volatile String txInterruptMsg = "";

    private AtomicLong txID = new AtomicLong(1);
    private volatile boolean isLocked = false;
    private long lastInsertId;
    private final NonBlockingSession session;
    private ServerSptPrepare sptprepare;
    private volatile RequestScope requestScope;
    protected volatile boolean setNoAutoCommit = false;

    public ShardingService(AbstractConnection connection, AuthResultInfo info) {
        super(connection, info);
        this.sptprepare = new ServerSptPrepare(this);
        this.handler = new ServerQueryHandler(this);
        this.loadDataInfileHandler = new ServerLoadDataInfileHandler(this);
        this.prepareHandler = new ServerPrepareHandler(this);
        this.session = new NonBlockingSession(this);
        session.setRowCount(0);
        this.protoLogicHandler = new MySQLProtoLogicHandler(this);
        this.shardingSQLHandler = new MySQLShardingSQLHandler(this);
        StatisticListener.getInstance().register(session);
    }

    public RequestScope getRequestScope() {
        return requestScope;
    }

    @Override
    public void handleVariable(MysqlVariable var) {
        String val = var.getValue();
        switch (var.getType()) {
            case XA:
                session.getTransactionManager().setXaTxEnabled(Boolean.parseBoolean(val), this);
                break;
            case TRACE:
                session.setTrace(Boolean.parseBoolean(val));
                break;
            case AUTOCOMMIT:
                if (Boolean.parseBoolean(val)) {
                    if (!autocommit) {
                        Optional.ofNullable(StatisticListener.getInstance().getRecorder(this)).ifPresent(r -> r.onTxEnd());
                        if (session.getTargetCount() > 0) {
                            setNoAutoCommit = true;
                            session.implicitCommit(() -> {
                                autocommit = true;
                                txStarted = false;
                                this.transactionsCount();
                                writeOkPacket();
                            });
                            return;
                        } else {
                            txStarted = false;
                            this.transactionsCount();
                        }
                    } else if (!txStarted) {
                        this.transactionsCount();
                    }
                    autocommit = true;
                } else {
                    if (autocommit) {
                        if (!txStarted) {
                            Optional.ofNullable(StatisticListener.getInstance().getRecorder(this)).ifPresent(r -> r.onTxStart(this));
                        }
                        autocommit = false;
                        txStarted = true;
                        TxnLogHelper.putTxnLog(this, executeSql);
                    }
                }
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

        WallProvider blackList = userConfig.getBlacklist();
        if (blackList != null) {
            WallCheckResult result = blackList.check(
                    ((ServerParseFactory.getShardingParser().parse(sql) & 0xff) == ServerParse.BEGIN) ? "start transaction" : sql);
            if (!result.getViolations().isEmpty()) {
                if (result.isSyntaxError()) {
                    LOGGER.info("{}", result.getViolations().get(0).getMessage());
                    writeErrMessage(ErrorCode.ER_PARSE_ERROR, "druid not support sql syntax, the reason is " +
                            result.getViolations().get(0).getMessage());
                } else {
                    LOGGER.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:" +
                            result.getViolations().get(0).getMessage() + " \r\n " + sql);
                    writeErrMessage(ErrorCode.ERR_WRONG_USED, "The statement is unsafe SQL, reject for user '" + user + "'");
                }
                return;
            }
        }

        SerializableLock.getInstance().lock(this.connection.getId());

        this.handler.setReadOnly(userConfig.isReadOnly());
        this.handler.query(sql);
    }

    @Override
    protected void beforeHandlingTask() {
        TraceManager.sessionStart(this, "sharding-server-start");
        session.setRequestTime();
    }

    @Override
    protected void handleInnerData(byte[] data) {
        getSession2().startProcess();
        try (RequestScope requestScope = new RequestScope()) {
            if (data[4] != MySQLPacket.COM_STMT_EXECUTE) {
                GeneralLogHelper.putGLog(this, data);
            }
            this.requestScope = requestScope;
            switch (data[4]) {
                case MySQLPacket.COM_STMT_PREPARE:
                case MySQLPacket.COM_STMT_EXECUTE:
                case MySQLPacket.COM_QUERY:
                    if (!connectionSerializableLock.tryLock()) {
                        LOGGER.error("connection is already locking. {}", this);
                        return;
                    }
                    break;
                default:
                    break;
            }
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
                case MySQLPacket.COM_STMT_FETCH:
                    commands.doStmtFetch();
                    this.stmtFetch(data);
                    break;
                case MySQLPacket.COM_SET_OPTION:
                    commands.doOther();
                    setOption(data);
                    break;
                case MySQLPacket.COM_CHANGE_USER:
                    commands.doOther();
                    final MySQLChangeUserService fService = new MySQLChangeUserService(connection, this);
                    connection.setService(fService);
                    fService.handleInnerData(data);
                    break;
                case MySQLPacket.COM_RESET_CONNECTION:
                    commands.doOther();
                    resetConnection();
                    writeOkPacket();
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
            LOGGER.warn("execute sql cause error", e);
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

    public void stmtFetch(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.fetch(data);
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

    @Override
    public void resetConnection() {
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
    public void writeErrMessage(String sqlState, String msg, int vendorCode) {
        byte packetId = (byte) this.getSession2().getPacketId().get();
        writeErrMessage(++packetId, vendorCode, sqlState, msg);
    }

    @Override
    public void markFinished() {
        if (session != null) {
            session.setStageFinished();
            if (session.isDiscard() || session.isKilled()) {
                session.setKilled(false);
                session.setDiscard(false);
            }
            Optional.ofNullable(StatisticListener.getInstance().getRecorder(session)).ifPresent(r -> r.onFrontendSqlEnd());
        }
    }

    public void beginInTx(String stmt) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            TxnLogHelper.putTxnLog(session.getShardingService(), "commit[because of " + stmt + "]");
            this.txChainBegin = true;
            session.commit();
            this.transactionsCount();
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
                loadDataInfileHandler.init();
                loadDataInfileHandler.start(sql);
            } catch (Exception e) {
                // back to the beginning state
                loadDataInfileHandler.clear();
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
        if (packet instanceof OkPacket) {
            Optional.ofNullable(StatisticListener.getInstance().getRecorder(session)).ifPresent(r -> r.onFrontendSetRows(((OkPacket) packet).getAffectedRows()));
        }
        boolean multiQueryFlag = session.multiStatementPacket(packet);
        markFinished();
        if (packet.isEndOfSession()) {
            //error finished do resource clean up
            session.resetMultiStatementStatus();
            for (BackendConnection backendConnection : session.getTargetMap().values()) {
                TraceManager.sessionFinish(backendConnection.getBackendService());
            }
            TraceManager.sessionFinish(this);
            packet.bufferWrite(connection);
            connectionSerializableLock.unLock();
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

            connectionSerializableLock.unLock();
            SerializableLock.getInstance().unLock(this.connection.getId());
        } else {
            packet.bufferWrite(connection);
        }
    }

    @Override
    public void writeWithBuffer(MySQLPacket packet, ByteBuffer buffer) {
        boolean multiQueryFlag = session.multiStatementPacket(packet);
        markFinished();
        if (packet.isEndOfSession()) {
            //error finished do resource clean up
            session.resetMultiStatementStatus();
            for (BackendConnection backendConnection : session.getTargetMap().values()) {
                TraceManager.sessionFinish(backendConnection.getBackendService());
            }
            TraceManager.sessionFinish(this);
            connectionSerializableLock.unLock();
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
            connectionSerializableLock.unLock();
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

    @Override
    public void cleanup() {
        super.cleanup();
        if (session != null) {
            TsQueriesCounter.getInstance().addToHistory(this);
            session.terminate();
        }
        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
        if (prepareHandler != null) {
            prepareHandler.clear();
        }
    }

    @Override
    public void killAndClose(String reason) {
        connection.close(reason);
        StatisticListener.getInstance().remove(session);
        if (!isTxStart() || session.getTransactionManager().getXAStage() == null) {
            //not a xa transaction ,close it
            session.kill();
        }
    }

    public NonBlockingSession getSession2() {
        return session;
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

    public ServerLoadDataInfileHandler getLoadDataInfileHandler() {
        return loadDataInfileHandler;
    }

    public ServerSptPrepare getSptPrepare() {
        return sptprepare;
    }

    public boolean isSetNoAutoCommit() {
        return setNoAutoCommit;
    }

    public void setSetNoAutoCommit(boolean setNoAutoCommit) {
        this.setNoAutoCommit = setNoAutoCommit;
    }

    public String toString() {
        return "ShardingService[ user = " + user + " schema = " + schema + " executeSql = " + executeSql + " txInterruptMsg = " + txInterruptMsg +
                " sessionReadOnly = " + sessionReadOnly + "] with connection " + connection.toString() + " with session " + session.toString();
    }
}
