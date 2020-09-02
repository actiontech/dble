package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
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
import com.actiontech.dble.server.response.Heartbeat;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.statistic.CommandCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RWSplitService extends MySQLBasedService implements FrontEndService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(RWSplitService.class);

    private final CommandCount commands;
    private final RWSplitQueryHandler queryHandler;
    private UserName user;
    private volatile String schema;
    private PhysicalDbGroup group;
    private volatile int txIsolation;
    private volatile boolean autocommit;
    protected String executeSql;

    public RWSplitService(AbstractConnection connection) {
        super(connection);
        this.commands = connection.getProcessor().getCommands();
        this.proto = new MySQLProtoHandlerImpl();
        this.queryHandler = new RWSplitQueryHandler(this);
    }

    public void initFromAuthInfo(AuthResultInfo info) {
        AuthPacket auth = info.getMysqlAuthPacket();
        this.user = new UserName(auth.getUser(), auth.getTenant());
        this.schema = info.getMysqlAuthPacket().getDatabase();
        this.userConfig = info.getUserConfig();
        this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        this.autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        //        this.handler.setReadOnly(((ManagerUserConfig) userConfig).isReadOnly());
        this.group = DbleServer.getInstance().getConfig().getDbGroups().get(((RwSplitUserConfig) userConfig).getDbGroup());
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
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB: {
                commands.doInitDB();
                MySQLMessage mm = new MySQLMessage(data);
                mm.position(5);
                String switchSchema;
                try {
                    switchSchema = mm.readString(getCharset().getClient());
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + getCharset().getClient() + "'");
                    return;
                }
                this.executeSql = "use `" + switchSchema + "`";
                execute(false, service -> service.setSchema(switchSchema));
                break;
            }
            case MySQLPacket.COM_QUERY: {
                commands.doQuery();
                MySQLMessage mm = new MySQLMessage(data);
                mm.position(5);
                try {
                    String sql = mm.readString(getCharset().getClient());
                    executeSql = sql;
                    queryHandler.query(sql);
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
                    return;
                }
                break;
            }
            // prepared statement
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                // todo
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                // todo
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                // todo
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
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
            // other statement push down to master
            default:
                commands.doOther();
                execute(false, null);
        }
    }

    public void execute(boolean canPushDown2Slave, Callback callback) {
        PhysicalDbInstance instance = group.select(canPushDown2Slave);
        try {
            instance.getConnection(this.schema, new RWSplitHandler(this, callback), null, false);
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

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getExecuteSql() {
        return executeSql;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    @Override
    public void killAndClose(String reason) {
        connection.close(reason);
    }
}
