package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.FrontEndService;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.ServerSptPrepare;
import com.actiontech.dble.server.handler.ServerLoadDataInfileHandler;
import com.actiontech.dble.server.handler.ServerPrepareHandler;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.SetInnerHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.services.mysqlsharding.handler.LoadDataProtoHandlerImpl;
import com.actiontech.dble.singleton.*;
import com.actiontech.dble.statistic.CommandCount;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by szf on 2020/6/18.
 */
public class ShardingService extends MySQLBasedService implements FrontEndService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);

    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<>();

    private final ServerQueryHandler handler;

    private final ServerLoadDataInfileHandler loadDataInfileHandler;

    private final FrontendPrepareHandler prepareHandler;

    private final MySQLProtoLogicHandler protoLogicHandler;

    private final MySQLShardingSQLHandler shardingSQLHandler;

    protected final CommandCount commands;

    protected String executeSql;
    protected UserName user;
    private long clientFlags;
    private volatile boolean autocommit;
    private volatile boolean txStarted;
    private volatile boolean txChainBegin;
    private volatile boolean txInterrupted;
    private volatile String txInterruptMsg = "";

    protected long lastReadTime;

    private volatile int txIsolation;

    private AtomicLong txID = new AtomicLong(1);

    private volatile boolean isLocked = false;

    private long lastInsertId;

    protected String schema;

    private volatile boolean multiStatementAllow = false;

    private final NonBlockingSession session;

    private boolean sessionReadOnly = false;

    private List<Pair<SetHandler.KeyType, Pair<String, String>>> contextTask = new ArrayList<>();
    private List<Pair<SetHandler.KeyType, Pair<String, String>>> innerSetTask = new ArrayList<>();

    private ServerSptPrepare sptprepare;

    public ShardingService(AbstractConnection connection) {
        super(connection);
        this.sptprepare = new ServerSptPrepare(this);
        this.handler = new ServerQueryHandler(this);
        this.loadDataInfileHandler = new ServerLoadDataInfileHandler(this);
        this.prepareHandler = new ServerPrepareHandler(this);
        this.session = new NonBlockingSession(this);
        session.setRowCount(0);
        this.commands = connection.getProcessor().getCommands();
        this.protoLogicHandler = new MySQLProtoLogicHandler(this);
        this.shardingSQLHandler = new MySQLShardingSQLHandler(this);
        this.proto = new MySQLProtoHandlerImpl();
        this.autocommit = SystemConfig.getInstance().getAutocommit() == 1;
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
                protoLogicHandler.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                connection.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                protoLogicHandler.kill(data);
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
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                prepareHandler.reset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                this.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                protoLogicHandler.heartbeat(data);
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


    public void stmtExecute(byte[] data, Queue<byte[]> dataqueue) {
        byte[] sendData = dataqueue.poll();
        while (sendData != null) {
            this.stmtSendLongData(sendData);
            sendData = dataqueue.poll();
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

    public void initCharsetIndex(int ci) {
        String name = CharsetUtil.getCharset(ci);
        if (name != null) {
            connection.setCharacterSet(name);
        }
    }

    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        this.shardingSQLHandler.routeSystemInfoAndExecuteSQL(stmt, schemaInfo, sqlType);
    }

    public void initFromAuthInfo(AuthResultInfo info) {

        AuthPacket auth = info.getMysqlAuthPacket();
        this.schema = auth.getDatabase();
        this.userConfig = info.getUserConfig();
        this.user = new UserName(auth.getUser(), auth.getTenant());

        SystemConfig sys = SystemConfig.getInstance();
        txIsolation = sys.getTxIsolation();

        this.initCharsetIndex(auth.getCharsetIndex());
        multiStatementAllow = auth.isMultStatementAllow();
        clientFlags = auth.getClientFlags();

        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            this.setSupportCompress(true);
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(this).append('\'').append(auth.getUser()).append("' login success");
            byte[] extra = auth.getExtra();
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.debug(s.toString());
        }
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

    public void beginInTx(String stmt) {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
        } else {
            TxnLogHelper.putTxnLog(session.getShardingService(), "commit[because of " + stmt + "]");
            this.txChainBegin = true;
            session.commit();
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
            TsQueriesCounter.getInstance().addToHistory(session);
            session.terminate();
        }
        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
    }

    protected void sessionStart() {
        TraceManager.sessionStart(this, "sharding-server-start");
    }


    public void setCollationConnection(String collation) {
        connection.getCharsetName().setCollation(collation);
    }

    public void setCharacterResults(String name) {
        connection.getCharsetName().setResults(name);
    }

    public void setCharacterConnection(String collationName) {
        connection.getCharsetName().setCollation(collationName);
    }

    public void setNames(String name, String collationName) {
        connection.getCharsetName().setNames(name, collationName);
    }

    public void setCharacterClient(String name) {
        connection.getCharsetName().setClient(name);
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public boolean isTxStarted() {
        return txStarted;
    }

    public void setTxStarted(boolean txStarted) {
        this.txStarted = txStarted;
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

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public boolean isTxStart() {
        return txStarted;
    }

    public UserName getUser() {
        return user;
    }

    public String getExecuteSql() {
        return executeSql;
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


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setCharacterSet(String name) {
        connection.setCharacterSet(name);
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


    public Map<String, String> getUsrVariables() {
        return usrVariables;
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

    public Map<String, String> getSysVariables() {
        return sysVariables;
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

    public ServerSptPrepare getSptPrepare() {
        return sptprepare;
    }

    @Override
    public void userConnectionCount() {
        FrontendUserManager.getInstance().countDown(user, false);
    }

    public void resetProto() {
        this.proto = new MySQLProtoHandlerImpl();
    }

    public long getClientFlags() {
        return clientFlags;
    }

    public String toBriefString() {
        return "Shardingservice";
    }
}
