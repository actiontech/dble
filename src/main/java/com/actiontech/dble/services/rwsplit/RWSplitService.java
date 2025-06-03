package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.CommandPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.trace.RwTraceResult;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.singleton.AppendTraceId;
import com.actiontech.dble.singleton.TsQueriesCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.actiontech.dble.net.mysql.MySQLPacket.COM_STMT_PREPARE;

public class RWSplitService extends BusinessService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RWSplitService.class);
    private static final Pattern HINT_DEST = Pattern.compile(".*/\\*\\s*dble_dest_expect\\s*:\\s*([M|S])\\s*\\*/", Pattern.CASE_INSENSITIVE);

    private volatile boolean isLocked;
    private volatile boolean inLoadData;
    private volatile boolean inPrepare;

    private volatile String executeSql;
    private volatile byte[] executeSqlBytes;
    // only for test
    private volatile String expectedDest;

    private final RWSplitQueryHandler queryHandler;
    private final RWSplitNonBlockingSession session;

    public static final int LOCK_READ = 2;

    AtomicInteger sqlUniqueId = new AtomicInteger(1);

    // prepare statement
    private ConcurrentHashMap<Long, PreparedStatementHolder> psHolder = new ConcurrentHashMap<>();

    public RWSplitService(AbstractConnection connection) {
        super(connection);
        this.session = new RWSplitNonBlockingSession(this);
        this.queryHandler = new RWSplitQueryHandler(session);
    }

    @Override
    public void handleVariable(MysqlVariable var) {
        switch (var.getType()) {
            case AUTOCOMMIT:
                String ac = var.getValue();
                if (autocommit && !Boolean.parseBoolean(ac)) {
                    autocommit = false;
                    txStarted = true;
                    writeOkPacket();
                    return;
                }
                if (!autocommit && Boolean.parseBoolean(ac)) {
                    session.execute(true, (isSuccess, resp, rwSplitService) -> {
                        session.getConn().getBackendService().setAutocommit(true);
                        rwSplitService.setAutocommit(true);
                        txStarted = false;
                        this.singleTransactionsCount();
                    });
                    return;
                }
                this.singleTransactionsCount();
                writeOkPacket();
                break;
            default:
                break;
        }
    }

    @Override
    public void initFromAuthInfo(AuthResultInfo info) {
        super.initFromAuthInfo(info);
        this.session.setRwGroup(DbleServer.getInstance().getConfig().getDbGroups().get(((RwSplitUserConfig) userConfig).getDbGroup()));
    }

    @Override
    protected void taskToTotalQueue(ServiceTask task) {
        session.setRequestTime();
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        session.startProcess();
        // if the statement is load data, directly push down
        if (inLoadData) {
            session.execute(true, data, (isSuccess, resp, rwSplitService) -> {
                rwSplitService.setInLoadData(false);
            });
            return;
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
            case COM_STMT_PREPARE:
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
                inPrepare = false;
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
            case MySQLPacket.COM_FIELD_LIST:
                commands.doOther();
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "unsupport statement");
                break;
            case MySQLPacket.COM_STATISTICS:
                commands.doOther();
                session.execute(null, data, null);
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
                if (isSuccess && SlowQueryLog.getInstance().isEnableSlowLog()) {
                    String sql = "use " + switchSchema;
                    if (AppendTraceId.getInstance().isEnable()) {
                        sql = String.format("/*+ trace_id=%d-%d */ %s", session.getService().getConnection().getId(), session.getService().getSqlUniqueId().incrementAndGet(), sql);
                    }
                    SlowQueryLog.getInstance().putSlowQueryLogForce(this.session.getService(), new RwTraceResult(), sql);
                }
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
            inPrepare = true;
            String sql = mm.readString(getCharset().getClient());
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1).trim();
            }
            sql = sql.trim();


            String tmpSql = sql;
            byte[] tmpData = data;
            if (AppendTraceId.getInstance().isEnable()) {
                tmpSql = String.format("/*+ trace_id=%d-%d */ %s", session.getService().getConnection().getId(), getSqlUniqueId().incrementAndGet(), sql);
                CommandPacket packet = new CommandPacket();
                packet.setCommand(COM_STMT_PREPARE);
                packet.setArg(tmpSql.getBytes());
                packet.setPacketId(data[3]);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                packet.write(out);
                tmpData = out.toByteArray();
            }

            int rs = ServerParse.parse(sql);
            int sqlType = rs & 0xff;
            final String finalSql = tmpSql;
            setExecuteSql(finalSql);
            final byte[] finalData = tmpData;
            session.endParse();

            switch (sqlType) {
                case ServerParse.SELECT:
                    int rs2 = ServerParse.parseSpecial(sqlType, sql);
                    if (rs2 == LOCK_READ) {
                        session.execute(true, finalData, (isSuccess, resp, rwSplitService) -> {
                            if (isSuccess) {
                                long statementId = ByteUtil.readUB4(resp, 5);
                                int paramCount = ByteUtil.readUB2(resp, 11);
                                psHolder.put(statementId, new PreparedStatementHolder(finalData, paramCount, true, finalSql));
                            }
                        }, false);
                    } else {
                        session.execute(null, finalData, (isSuccess, resp, rwSplitService) -> {
                            if (isSuccess) {
                                long statementId = ByteUtil.readUB4(resp, 5);
                                int paramCount = ByteUtil.readUB2(resp, 11);
                                psHolder.put(statementId, new PreparedStatementHolder(finalData, paramCount, false, finalSql));
                            }
                        }, false);
                    }
                    break;
                default:
                    session.execute(true, data, (isSuccess, resp, rwSplitService) -> {
                        if (isSuccess) {
                            long statementId = ByteUtil.readUB4(resp, 5);
                            int paramCount = ByteUtil.readUB2(resp, 11);
                            psHolder.put(statementId, new PreparedStatementHolder(finalData, paramCount, true, finalSql));
                        }
                    });
                    break;
            }
        } catch (IOException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        }
    }

    private void execute(byte[] data) {
        session.execute(true, data, null);
    }

    public AtomicInteger getSqlUniqueId() {
        return sqlUniqueId;
    }

    public RwSplitUserConfig getUserConfig() {
        return (RwSplitUserConfig) userConfig;
    }

    public RWSplitNonBlockingSession getSession() {
        return session;
    }

    @Override
    public String getExecuteSql() {
        return executeSql;
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
    }


    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public boolean isInLoadData() {
        return inLoadData;
    }

    public void setInLoadData(boolean inLoadData) {
        this.inLoadData = inLoadData;
    }

    public byte[] getExecuteSqlBytes() {
        return executeSqlBytes;
    }

    public void setExecuteSqlBytes(byte[] executeSqlBytes) {
        this.executeSqlBytes = executeSqlBytes;
    }

    public boolean isInPrepare() {
        return inPrepare;
    }

    public void setInPrepare(boolean inPrepare) {
        this.inPrepare = inPrepare;
    }

    public String getExpectedDest() {
        return expectedDest;
    }

    @Override
    public void setTxStart(boolean txStart) {
        this.txStarted = txStart;
    }

    @Override
    public void killAndClose(String reason) {
        session.close(reason);
        connection.close(reason);
    }

    public void cleanup() {
        super.cleanup();
        if (session != null) {
            TsQueriesCounter.getInstance().addToHistory(this);
            session.close("clean up");
        }
    }


    public PreparedStatementHolder getPrepareStatement(long id) {
        return psHolder.get(id);
    }

    public ConcurrentHashMap<Long, PreparedStatementHolder> getPsHolder() {
        return psHolder;
    }

}
