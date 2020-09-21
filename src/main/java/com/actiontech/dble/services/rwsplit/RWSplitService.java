package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.FrontEndService;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.statistic.CommandCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RWSplitService extends MySQLBasedService implements FrontEndService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(RWSplitService.class);

    private volatile String schema;
    private volatile int txIsolation;
    private volatile boolean autocommit;
    private volatile boolean isLocked;
    private volatile boolean txStart;
    private volatile boolean inLoadData;
    private volatile boolean inPrepare;

    private volatile String executeSql;
    private UserName user;

    private final CommandCount commands;
    private final RWSplitQueryHandler queryHandler;
    private final RWSplitNonBlockingSession session;

    public RWSplitService(AbstractConnection connection) {
        super(connection);
        this.commands = connection.getProcessor().getCommands();
        this.session = new RWSplitNonBlockingSession(this);
        this.queryHandler = new RWSplitQueryHandler(session);
    }

    public void initFromAuthInfo(AuthResultInfo info) {
        AuthPacket auth = info.getMysqlAuthPacket();
        this.user = new UserName(auth.getUser(), auth.getTenant());
        this.schema = info.getMysqlAuthPacket().getDatabase();
        this.userConfig = info.getUserConfig();
        this.session.setRwGroup(DbleServer.getInstance().getConfig().getDbGroups().get(((RwSplitUserConfig) userConfig).getDbGroup()));
        this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        this.autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.connection.initCharsetIndex(info.getMysqlAuthPacket().getCharsetIndex());
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

    @Override
    protected void taskToTotalQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        // if the statement is load data, directly push down
        if (inLoadData) {
            try {
                session.execute(true, data, null);
            } catch (IOException e) {
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
            }
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
                try {
                    session.getService().setInPrepare(false);
                    session.execute(true, data, null);
                } catch (IOException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
                }
                break;
            // connection
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                connection.close("quit cmd");
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
            session.execute(true, data, rwSplitService -> rwSplitService.setSchema(switchSchema));
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + getCharset().getClient() + "'");
        } catch (IOException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        }
    }

    private void handleComQuery(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        try {
            String sql = mm.readString(getCharset().getClient());
            executeSql = sql;
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
            int rs = ServerParse.parse(sql);
            int sqlType = rs & 0xff;
            switch (sqlType) {
                case ServerParse.SELECT:
                    session.execute(false, data, null);
                    break;
                default:
                    session.execute(true, data, null);
                    break;
            }
        } catch (IOException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        }
    }

    private void execute(byte[] data) {
        try {
            session.execute(true, data, null);
        } catch (IOException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        }
    }

    @Override
    public void userConnectionCount() {
        FrontendUserManager.getInstance().countDown(user, false);
    }

    @Override
    public UserName getUser() {
        return user;
    }

    public String getSchema() {
        return schema;
    }

    public RWSplitNonBlockingSession getSession() {
        return session;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getExecuteSql() {
        return executeSql;
    }

    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public boolean isTxStart() {
        return txStart;
    }

    public void setTxStart(boolean txStart) {
        this.txStart = txStart;
    }

    public boolean isInLoadData() {
        return inLoadData;
    }

    public void setInLoadData(boolean inLoadData) {
        this.inLoadData = inLoadData;
    }

    public boolean isInPrepare() {
        return inPrepare;
    }

    public void setInPrepare(boolean inPrepare) {
        this.inPrepare = inPrepare;
    }

    @Override
    public void killAndClose(String reason) {
        session.close(reason);
        connection.close(reason);
    }
}
