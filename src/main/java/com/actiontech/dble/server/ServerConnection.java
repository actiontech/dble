/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ServerUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.AuthUtil;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.handler.LoadDataInfileHandler;
import com.actiontech.dble.net.handler.ServerUserAuthenticator;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.SetInnerHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.FieldList;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.response.ShowCreateView;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.SerializableLock;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);

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

    private FrontendPrepareHandler prepareHandler;
    private LoadDataInfileHandler loadDataInfileHandler;
    private boolean sessionReadOnly = false;
    private volatile boolean multiStatementAllow = false;
    private ServerUserConfig userConfig;

    public ServerConnection(NetworkChannel channel) throws IOException {
        super(channel);

        this.handler = new ServerUserAuthenticator(this);
        this.txInterrupted = false;
        this.autocommit = SystemConfig.getInstance().getAutocommit() == 1;
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


    public ServerUserConfig getUserConfig() {
        return userConfig;
    }

    public void setUserConfig(ServerUserConfig userConfig) {
        this.userConfig = userConfig;
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

    public void setSession2(NonBlockingSession session2) {
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

    public LoadDataInfileHandler getLoadDataInfileHandler() {
        return loadDataInfileHandler;
    }

    public void setLoadDataInfileHandler(LoadDataInfileHandler loadDataInfileHandler) {
        this.loadDataInfileHandler = loadDataInfileHandler;
    }

    public boolean isMultiStatementAllow() {
        return multiStatementAllow;
    }

    public void setMultiStatementAllow(boolean multiStatementAllow) {
        this.multiStatementAllow = multiStatementAllow;
    }

    public void setPrepareHandler(FrontendPrepareHandler prepareHandler) {
        this.prepareHandler = prepareHandler;
    }

    public void setSessionReadOnly(boolean sessionReadOnly) {
        this.sessionReadOnly = sessionReadOnly;
    }

    public boolean isReadOnly() {
        return sessionReadOnly;
    }


    public void setSchema(String schema) {
        if (schema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        this.schema = schema;
    }


    public void changeUserAuthSwitch(byte[] data, ChangeUserPacket changeUserPacket) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("changeUser AuthSwitch request");
        }
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        changeUserPacket.setPassword(authSwitchResponse.getAuthPluginData());
        String errMsg = AuthUtil.authority(this, new UserName(changeUserPacket.getUser(), changeUserPacket.getTenant()), changeUserPacket.getPassword(), changeUserPacket.getDatabase(), false);
        byte packetId = (byte) (authSwitchResponse.getPacketId() + 1);
        if (errMsg == null) {
            changeUserSuccess(changeUserPacket, packetId);
        } else {
            writeErrMessage(packetId, ErrorCode.ER_ACCESS_DENIED_ERROR, errMsg);
        }
    }

    private void changeUserSuccess(ChangeUserPacket newUser, byte packetId) {
        UserName user = new UserName(newUser.getUser(), newUser.getTenant());
        this.setUser(user);
        this.setUserConfig((ServerUserConfig) DbleServer.getInstance().getConfig().getUsers().get(user));
        this.setSchema(newUser.getDatabase());
        this.initCharsetIndex(newUser.getCharsetIndex());
        OkPacket ok = new OkPacket();
        ok.read(OkPacket.OK);
        ok.setPacketId(packetId);
        ok.write(this);
    }

    @Override
    protected void setRequestTime() {
        if (session != null) {
            session.setRequestTime();
        }
    }

    @Override
    public void startProcess() {
        session.startProcess();
    }

    @Override
    public void markFinished() {
        if (session != null) {
            session.setStageFinished();
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
        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(this.user));
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
                if (schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable()) == null) {
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

    private void routeEndExecuteSQL(String sql, int type, SchemaConfig schemaConfig) {
        if (session.isKilled()) {
            writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
            return;
        }

        RouteResultset rrs;
        try {
            rrs = RouteService.getInstance().route(schemaConfig, type, sql, this);
            if (rrs == null) {
                return;
            }
            if (rrs.getSqlType() == ServerParse.DDL && rrs.getSchema() != null) {
                if (ProxyMeta.getInstance().getTmManager().getCatalogs().get(rrs.getSchema()).getView(rrs.getTable()) != null) {
                    ProxyMeta.getInstance().getTmManager().removeMetaLock(rrs.getSchema(), rrs.getTable());
                    String msg = "Table '" + rrs.getTable() + "' already exists as a view";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
            }
        } catch (Exception e) {
            executeException(e, sql);
            return;
        }

        session.endRoute(rrs);
        session.execute(rrs);
    }

    public void initDB(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String db = null;
        try {
            db = mm.readString(charsetName.getClient());
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charsetName.getClient() + "'");
            return;
        }
        if (db != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            db = db.toLowerCase();
        }
        // check sharding
        if (db == null || !DbleServer.getInstance().getConfig().getSchemas().containsKey(db)) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }
        if (userConfig instanceof ShardingUserConfig && !((ShardingUserConfig) userConfig).getSchemas().contains(db)) {
            String s = "Access denied for user '" + user + "' to database '" + db + "'";
            writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
            return;
        }
        this.schema = db;
        session.setRowCount(0);
        write(writeToBuffer(OkPacket.OK, allocate()));
    }


    public void loadDataInfileStart(String sql) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.start(sql);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }

        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile sql is not  unsupported!");
        }
    }

    public void loadDataInfileData(byte[] data) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.handle(data);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile  data is not  unsupported!");
        }

    }

    public void loadDataInfileEnd(byte packID) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.end(packID);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile end is not  unsupported!");
        }
    }


    public void stmtPrepare(byte[] data) {
        if (prepareHandler != null) {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            String sql = null;
            try {
                sql = mm.readString(charsetName.getClient());
            } catch (UnsupportedEncodingException e) {
                writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
                        "Unknown charset '" + charsetName.getClient() + "'");
                return;
            }
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }

            // record SQL
            this.setExecuteSql(sql);

            // execute
            prepareHandler.prepare(sql);
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

    public void setOption(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data); //see sql\protocol_classic.cc parse_packet
        if (mm.length() == 7) {
            mm.position(5);
            int optCommand = mm.readUB2();
            if (optCommand == 0) {
                this.multiStatementAllow = true;
                write(writeToBuffer(EOFPacket.EOF, allocate()));
                return;
            } else if (optCommand == 1) {
                this.multiStatementAllow = false;
                write(writeToBuffer(EOFPacket.EOF, allocate()));
                return;
            }
        }
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Set Option ERROR!");
    }

    //  mysql-server\sql\sql_class.cc void THD::cleanup_connection(void)
    public void resetConnection() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("resetConnection request");
        }
        this.innerCleanUp();
        this.write(OkPacket.OK);
    }

    public void changeUser(byte[] data, ChangeUserPacket changeUserPacket, AtomicBoolean isAuthSwitch) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("changeUser request");
        }
        this.innerCleanUp();
        changeUserPacket.read(data);
        if ("mysql_native_password".equals(changeUserPacket.getAuthPlugin())) {
            AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(changeUserPacket.getAuthPlugin().getBytes(), this.getSeed());
            authSwitch.setPacketId(changeUserPacket.getPacketId() + 1);
            isAuthSwitch.set(true);
            authSwitch.write(this);
        } else {
            writeErrMessage((byte) (changeUserPacket.getPacketId() + 1), ErrorCode.ER_PLUGIN_IS_NOT_LOADED, "NOT SUPPORT THIS PLUGIN!");
        }
    }

    public void stmtReset(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.reset(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
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

    public void stmtClose(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.close(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }


    public void rawHandle(final byte[] data) {

        //load data infile  client send empty packet which size is 4
        if (data.length == 4 && data[0] == 0 && data[1] == 0 && data[2] == 0) {
            // load in data empty packet
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    loadDataInfileEnd(data[3]);
                }
            });
            return;
        }
        //when TERMINATED char of load data infile is \001
        if (data.length > 4 && data[0] == 1 && data[1] == 0 && data[2] == 0 && data[3] == 0 && data[4] == MySQLPacket.COM_QUIT) {
            this.getProcessor().getCommands().doQuit();
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    close("quit cmd");
                }
            });
            return;
        }
        handler.handle(data);

    }

    public void kill(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void fieldList(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        FieldList.response(this, mm.readStringWithNull());
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
            session.implicitCommit(() -> doLockTable(sql));
            return;
        }
        doLockTable(sql);
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
        autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        txIsolation = SystemConfig.getInstance().getTxIsolation();
        this.setCharacterSet(SystemConfig.getInstance().getCharset());
        lastInsertId = 0;

        //prepare
        if (prepareHandler != null) {
            prepareHandler.clear();
        }
    }

    @Override
    public void handlerQuery(String sql) {
        WallProvider blackList = userConfig.getBlacklist();
        if (blackList != null) {
            WallCheckResult result = blackList.check(sql);
            if (!result.getViolations().isEmpty()) {
                LOGGER.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:" +
                        result.getViolations().get(0).getMessage() + " \r\n " + sql);
                writeErrMessage(ErrorCode.ERR_WRONG_USED, "The statement is unsafe SQL, reject for user '" + user + "'");
                return;
            }
        }

        SerializableLock.getInstance().lock(this.id);
        // execute
        if (queryHandler != null) {
            boolean readOnly = false;
            if (userConfig instanceof ShardingUserConfig) {
                readOnly = ((ShardingUserConfig) userConfig).isReadOnly();
            }
            queryHandler.setReadOnly(readOnly);
            queryHandler.setSessionReadOnly(sessionReadOnly);
            queryHandler.query(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Query unsupported!");
        }
    }

    @Override
    public void handle(final byte[] data) {
        setRequestTime();
        if (isSupportCompress()) {
            List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, decompressUnfinishedDataQueue);
            for (byte[] pack : packs) {
                if (pack.length != 0) {
                    rawHandle(pack);
                }
            }

        } else {
            rawHandle(data);
        }
    }

    @Override
    public synchronized void close(String reason) {
        if (isClosed) {
            return;
        }
        super.close(reason);
        if (session != null) {
            TsQueriesCounter.getInstance().addToHistory(session);
            session.terminate();
        }
        if (getLoadDataInfileHandler() != null) {
            getLoadDataInfileHandler().clear();
        }
        SerializableLock.getInstance().unLock(id);
    }

    @Override
    public void killAndClose(String reason) {
        super.close(reason);
        if (!session.getSource().isTxStart() || session.getTransactionManager().getXAStage() == null) {
            //not a xa transaction ,close it
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
        result.append(",port=");
        result.append(port);
        result.append(", user=");
        result.append(user);
        result.append(",txIsolation=");
        result.append(txIsolation);
        result.append(", autocommit=");
        result.append(autocommit);
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
        if (session.isPrepared()) {
            session.setPrepared(false);
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
        if (session.isPrepared()) {
            session.setPrepared(false);
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
        if (session.isPrepared()) {
            session.setPrepared(false);
        }
    }

    @Override
    public final void write(ByteBuffer buffer) {
        SerializableLock.getInstance().unLock(this.id);
        markFinished();
        super.write(buffer);
        if (session != null) {
            if (session.isDiscard() || session.isKilled()) {
                session.setKilled(false);
                session.setDiscard(false);
            }
            if (session.isPrepared()) {
                session.setPrepared(false);
            }
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
