package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.log.general.GeneralLogHelper;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.NormalServiceTask;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.net.service.ServiceTaskType;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.parser.RwSplitServerParse;
import com.actiontech.dble.server.parser.RwSplitServerParseSelect;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLChangeUserService;
import com.actiontech.dble.services.rwsplit.handle.PreparedStatementHolder;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.statistic.sql.StatisticListener;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RWSplitService extends BusinessService<RwSplitUserConfig> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RWSplitService.class);
    private static final Pattern HINT_DEST = Pattern.compile(".*/\\*\\s*dble_dest_expect\\s*:\\s*([M|S])\\s*\\*/", Pattern.CASE_INSENSITIVE);

    private volatile boolean inLoadData;
    private volatile boolean firstInLoadData = true;
    private volatile Set<String/* schemaName.tableName */> tmpTableSet;
    private final Set<String> nameSet = new HashSet<>();
    private volatile byte[] executeSqlBytes;
    // only for test
    private volatile String expectedDest;

    private final RWSplitQueryHandler queryHandler;
    private final RWSplitNonBlockingSession session;

    private ConcurrentHashMap<Long, PreparedStatementHolder> psHolder = new ConcurrentHashMap<>();

    public RWSplitService(AbstractConnection connection, AuthResultInfo info) {
        super(connection, info);
        this.session = new RWSplitNonBlockingSession(this);
        this.session.setRwGroup(DbleServer.getInstance().getConfig().getDbGroups().get(userConfig.getDbGroup()));
        this.queryHandler = new RWSplitQueryHandler(session);
        StatisticListener.getInstance().register(session);
    }

    @Override
    public void handleVariable(MysqlVariable var) {
        switch (var.getType()) {
            case AUTOCOMMIT:
                if (Boolean.parseBoolean(var.getValue())) {
                    if (!autocommit) {
                        StatisticListener.getInstance().record(this, r -> r.onTxEnd());
                        session.execute(true, (isSuccess, resp, rwSplitService) -> {
                            session.getConn().getBackendService().setAutocommit(true);
                            rwSplitService.setAutocommit(true);
                            rwSplitService.setTxStart(false);
                            this.transactionsCount();
                        });
                        return;
                    } else if (!txStarted) {
                        this.transactionsCount();
                    }
                } else {
                    if (autocommit) {
                        StatisticListener.getInstance().record(this, r -> r.onTxStartBySet(this));
                        autocommit = false;
                        txStarted = true;
                        StatisticListener.getInstance().record(this, r -> r.onFrontendSqlEnd());
                        writeOkPacket();
                        return;
                    }
                }
                StatisticListener.getInstance().record(this, r -> r.onFrontendSqlEnd());
                writeOkPacket();
                break;
            default:
                break;
        }
    }


    @Override
    protected boolean beforeHandlingTask(@NotNull ServiceTask task) {
        TraceManager.sessionStart(this, "rwSplit-server-start");
        if (task.getType() == ServiceTaskType.NORMAL) {
            final int packetType = ((NormalServiceTask) task).getPacketType();
            if (packetType == MySQLPacket.COM_STMT_PREPARE || packetType == MySQLPacket.COM_STMT_EXECUTE || packetType == MySQLPacket.COM_QUERY) {
                StatisticListener.getInstance().record(session, r -> r.onFrontendSqlStart());
            }
        }
        return true;
    }

    @Override
    protected void handleInnerData(byte[] data) {
        // TODO need to consider COM_STMT_EXECUTE
        GeneralLogHelper.putGLog(this, data);
        // if the statement is load data, directly push down
        if (inLoadData) {
            session.execute(true, data, (isSuccess, resp, rwSplitService) -> {
                rwSplitService.setInLoadData(false);
            });
            return;
        }
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
                handleComInitDb(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                handleComQuery(data);
                break;
            // prepared statement
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                handleComStmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                execute(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                execute(data);
                break;
            case MySQLPacket.COM_STMT_SEND_LONG_DATA:
                commands.doStmtSendLongData();
                execute(data);
                break;
            case MySQLPacket.COM_STMT_CLOSE:
                commands.doStmtClose();
                session.execute(true, data, null);
                // COM_STMT_CLOSE No response is sent back to the client.
                long statementId = ByteUtil.readUB4(data, 5);
                psHolder.remove(statementId);
                session.unbindIfSafe();
                break;
            // connection
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                session.close("quit cmd");
                connection.close("front conn receive quit cmd");
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                Heartbeat.response(connection, data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                Ping.response(connection);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                setOption(data);
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                resetConnection();
                writeOkPacket();
                break;
            case MySQLPacket.COM_CHANGE_USER:
                commands.doOther();
                final MySQLChangeUserService fService = new MySQLChangeUserService(connection, this);
                connection.setService(fService);
                fService.consumeSingleTask(new NormalServiceTask(data, this, 0));
                break;
            default:
                commands.doOther();
                // other statement push down to master
                execute(data);
                break;
        }
    }

    private void handleComInitDb(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String switchSchema;
        try {
            switchSchema = mm.readString(getCharset().getClient());
            session.execute(true, data, (isSuccess, resp, rwSplitService) -> {
                if (isSuccess) rwSplitService.setSchema(switchSchema);
            });
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + getCharset().getClient() + "'");
        }
    }

    private void handleComQuery(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        try {
            String sql = mm.readString(getCharset().getClient());
            if (LOGGER.isDebugEnabled()) {
                Matcher match = HINT_DEST.matcher(sql);
                if (match.matches()) {
                    expectedDest = match.group(1);
                } else {
                    expectedDest = null;
                }
            }
            executeSql = sql;
            executeSqlBytes = data;
            queryHandler.query(sql);
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        }
    }

    private void handleComStmtPrepare(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        try {
            RwSplitServerParse serverParse = ServerParseFactory.getRwSplitParser();
            String sql = mm.readString(getCharset().getClient());
            int rs = serverParse.parse(sql);
            int sqlType = rs & 0xff;
            if (sqlType == ServerParse.SELECT) {
                int rs2 = RwSplitServerParseSelect.parseSpecial(sql);
                if (rs2 == RwSplitServerParseSelect.LOCK_READ) {
                    session.execute(true, data, (isSuccess, resp, rwSplitService) -> {
                        if (isSuccess) {
                            long statementId = ByteUtil.readUB4(resp, 5);
                            int paramCount = ByteUtil.readUB2(resp, 11);
                            psHolder.put(statementId, new PreparedStatementHolder(data, paramCount, true));
                        }
                    }, false);
                } else {
                    session.execute(null, data, (isSuccess, resp, rwSplitService) -> {
                        if (isSuccess) {
                            long statementId = ByteUtil.readUB4(resp, 5);
                            int paramCount = ByteUtil.readUB2(resp, 11);
                            psHolder.put(statementId, new PreparedStatementHolder(data, paramCount, false));
                        }
                    }, false);
                }
            } else {
                session.execute(true, data, (isSuccess, resp, rwSplitService) -> {
                    if (isSuccess) {
                        long statementId = ByteUtil.readUB4(resp, 5);
                        int paramCount = ByteUtil.readUB2(resp, 11);
                        psHolder.put(statementId, new PreparedStatementHolder(data, paramCount, true));
                    }
                });
            }
        } catch (IOException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        }
    }

    private void execute(byte[] data) {
        session.execute(true, data, null);
    }

    public RWSplitNonBlockingSession getSession2() {
        return session;
    }

    @Override
    public Session getSession() {
        return session;
    }

    public byte[] getExecuteSqlBytes() {
        return executeSqlBytes;
    }

    public void implicitlyDeal() {
        if (!this.isAutocommit()) {
            StatisticListener.getInstance().record(session, r -> r.onTxEnd());
            this.getAndIncrementTxId();
            StatisticListener.getInstance().record(session, r -> r.onTxStartByImplicitly(this));
        }
        if (this.isTxStart()) {
            StatisticListener.getInstance().record(session, r -> r.onTxEnd());
        }
        if (this.isTxStart() || !this.isAutocommit()) {
            this.setTxStart(false);
            this.transactionsCount();
        }
    }

    public boolean isInLoadData() {
        return inLoadData;
    }

    public void setInLoadData(boolean inLoadData) {
        if (inLoadData) {
            this.firstInLoadData = true;
        }
        this.inLoadData = inLoadData;
    }

    public boolean isFirstInLoadData() {
        if (firstInLoadData) {
            firstInLoadData = false;
            return true;
        }
        return false;
    }

    public boolean isUsingTmpTable() {
        if (tmpTableSet == null) {
            return false;
        }
        return !tmpTableSet.isEmpty();
    }

    public Set<String> getTmpTableSet() {
        if (tmpTableSet == null) {
            synchronized (this) {
                if (tmpTableSet == null) {
                    tmpTableSet = ConcurrentHashMap.newKeySet();
                }
                return tmpTableSet;
            }
        }
        return tmpTableSet;
    }

    public String getExpectedDest() {
        return expectedDest;
    }

    public Set<String> getNameSet() {
        return nameSet;
    }

    @Override
    public void setTxStart(boolean txStart) {
        this.txStarted = txStart;
    }

    public PreparedStatementHolder getPrepareStatement(long id) {
        return psHolder.get(id);
    }

    public boolean isKeepBackendConn() {
        return isAutocommit() && !isTxStart() && !isInLoadData() && psHolder.isEmpty() && !isLockTable() && !isUsingTmpTable() && nameSet.isEmpty();
    }

    @Override
    public void killAndClose(String reason) {
        session.close(reason);
        connection.close(reason);
        StatisticListener.getInstance().remove(session);
    }

    @Override
    public void resetConnection() {
        session.close("reset connection");

        setLockTable(false);
        inLoadData = false;
        txStarted = false;
        this.tmpTableSet.clear();
        this.sysVariables.clear();
        this.usrVariables.clear();
        this.psHolder.clear();
        autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        txIsolation = SystemConfig.getInstance().getTxIsolation();
        setCharacterSet(SystemConfig.getInstance().getCharset());
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (session != null) {
            TsQueriesCounter.getInstance().addToHistory(this);
            session.close("clean up");
        }
    }
}
