/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.btrace.provider.GeneralProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.WallErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.log.general.GeneralLogHelper;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.*;
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
import com.actiontech.dble.singleton.SerializableLock;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.exception.NeedDelayedException;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


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

    private long lastInsertId;
    @Nonnull
    private final NonBlockingSession session;
    private final ServerSptPrepare sptprepare;
    private volatile RequestScope requestScope;
    private volatile boolean setNoAutoCommit = false;

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
                        TxnLogHelper.putTxnLog(this, executeSql);
                        StatisticListener.getInstance().record(this, r -> r.onTxEnd());
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
                            StatisticListener.getInstance().record(this, r -> r.onTxStart(this));
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
        if (blacklistCheck(sql, blackList)) return;
        SerializableLock.getInstance().lock(this.connection.getId());

        this.handler.setReadOnly(userConfig.isReadOnly());
        this.handler.query(sql);
    }

    private boolean blacklistCheck(String sql, WallProvider blackList) {
        if (Objects.isNull(blackList)) return false;
        WallCheckResult result = blackList.check(
                ((ServerParseFactory.getShardingParser().parse(sql) & 0xff) == ServerParse.BEGIN) ? "start transaction" : sql);
        if (!result.getViolations().isEmpty()) {
            if (result.isSyntaxError()) {
                LOGGER.info("{}", result.getViolations().get(0).getMessage());
                writeErrMessage(ErrorCode.ER_PARSE_ERROR, "druid not support sql syntax, the reason is " +
                        result.getViolations().get(0).getMessage());
            } else {
                String violation = "[" + WallErrorCode.get(result.getViolations().get(0)) + "]";
                String msg = "Intercepted by suspected configuration " + violation + " in the blacklist of user '" + user.getFullName() + "', so it is considered unsafe SQL";
                LOGGER.warn("Firewall message:{}, {}",
                        result.getViolations().get(0).getMessage(), msg);
                writeErrMessage(ErrorCode.ERR_WRONG_USED, msg);
            }
            return true;
        }
        return false;
    }

    @Override
    protected boolean beforeHandlingTask(@NotNull ServiceTask task) {
        TraceManager.sessionStart(this, "sharding-server-start");
        if (task.getType() == ServiceTaskType.NORMAL) {
            final int packetType = ((NormalServiceTask) task).getPacketType();
            if (packetType == MySQLPacket.COM_STMT_PREPARE || packetType == MySQLPacket.COM_STMT_EXECUTE || packetType == MySQLPacket.COM_QUERY) {
                StatisticListener.getInstance().record(this, r -> r.onFrontendSqlStart());
            }
        }
        session.setRequestTime();
        return true;
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
                    GeneralProvider.beforeChangeUserSuccess();
                    fService.consumeSingleTask(new NormalServiceTask(data, this, 0));
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

        } catch (NeedDelayedException e) {
            throw e;
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

        setLockTable(false);
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


    public void beginInTx(String stmt) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            if (session.getTransactionManager().isXaEnabled()) {
                getClusterDelayService().markDoingOrDelay(true);
            }
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
            if (session.getTransactionManager().isXaEnabled()) {
                getClusterDelayService().markDoingOrDelay(true);
            }
            if (session.getShardingService().isTxStart() || !session.getShardingService().isAutocommit()) {
                TxnLogHelper.putTxnLog(session.getShardingService(), logReason);
            }
            session.commit();
        }
    }

    public void rollback() {
        if (session.getTransactionManager().isXaEnabled()) {
            getClusterDelayService().markDoingOrDelay(true);
        }
        if (txInterrupted) {
            txInterrupted = false;
        }

        session.rollback();
    }

    public void unLockTable(String sql) {
        sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
        String[] words = SplitUtil.split(sql, ' ', true);
        if (words.length == 2 && ("table".equalsIgnoreCase(words[1]) || "tables".equalsIgnoreCase(words[1]))) {
            setLockTable(false);
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


    @Override
    public void beforeWriteFinish(@NotNull EnumSet<WriteFlag> writeFlags, ResultFlag resultFlag) {
        for (BackendConnection backendConnection : session.getTargetMap().values()) {
            TraceManager.sessionFinish(backendConnection.getBackendService());
        }


        if (writeFlags.contains(WriteFlag.END_OF_QUERY)) {
            if (session.getIsMultiStatement().get()) {
                TraceManager.queryFinish(this);
            } else {
                TraceManager.sessionFinish(this);
            }
        } else if (writeFlags.contains(WriteFlag.END_OF_SESSION)) {
            TraceManager.sessionFinish(this);
        }
        session.setResponseTime((resultFlag == ResultFlag.OK || resultFlag == ResultFlag.EOF_ROW));
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
        StatisticListener.getInstance().record(session, r -> r.onFrontendSqlEnd());
    }

    @Override
    public void afterWriteFinish(@NotNull EnumSet<WriteFlag> writeFlags) {
        if (writeFlags.contains(WriteFlag.END_OF_QUERY)) {
            multiStatementNextSql(session.getIsMultiStatement().get());
        } else if (writeFlags.contains(WriteFlag.END_OF_SESSION)) {
            session.resetMultiStatementStatus();
        }


        SerializableLock.getInstance().unLock(this.connection.getId());
        super.afterWriteFinish(writeFlags);
    }


    @Override
    public void beforePacket(MySQLPacket packet) {
        if (packet instanceof OkPacket) {
            StatisticListener.getInstance().record(session, r -> r.onFrontendSetRows(((OkPacket) packet).getAffectedRows()));
        }
        session.multiStatementPacket(packet);
    }

    public void multiStatementNextSql(boolean flag) {
        if (flag) {
            taskMultiQueryCreate(protoLogicHandler.getMultiQueryData());
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        session.terminate();
        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
        if (prepareHandler != null) {
            prepareHandler.clear();
        }
        SerializableLock.getInstance().unLock(connection.getId());
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

    @Override
    public Session getSession() {
        return session;
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

    public boolean isTxInterrupted() {
        return txInterrupted;
    }

    public String getTxInterruptMsg() {
        return txInterruptMsg;
    }

    public String toString() {
        String tmpSql = null;
        if (executeSql != null) {
            tmpSql = executeSql.length() > 1024 ? executeSql.substring(0, 1024) + "..." : executeSql;
        }

        return "ShardingService[ user = " + user + " schema = " + schema + " executeSql = " + tmpSql + " txInterruptMsg = " + txInterruptMsg +
                " sessionReadOnly = " + sessionReadOnly + "] with connection " + connection.toString() + " with session " + session.toString();
    }
}
