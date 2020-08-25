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
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.FrontEndService;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.statistic.CommandCount;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RWSplitService extends MySQLBasedService implements FrontEndService {

    private final CommandCount commands;
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
                String db = null;
                try {
                    db = mm.readString(getCharset().getClient());
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + getCharset().getClient() + "'");
                    return;
                }
                this.schema = db;
                writeDirectly(OkPacket.OK);
                break;
            }
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                connection.close("quit cmd");
                break;
            case MySQLPacket.COM_QUERY: {
                commands.doQuery();
                String sql;
                try {
                    MySQLMessage mm = new MySQLMessage(data);
                    mm.position(5);
                    sql = mm.readString(getCharset().getClient());
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
                    return;
                }
                executeSql = sql;
                int sqlType = ServerParse.parse(sql);
                PhysicalDbInstance instance = null;
                if ((sqlType & 0xff) == ServerParse.SELECT || sqlType == ServerParse.SHOW) {
                    instance = group.select(true);
                } else if (sqlType == ServerParse.DDL) {
                    instance = group.select(false);
                } else {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "not support");
                    return;
                }

                try {
                    instance.getConnection(this.schema, new RWSplitHandler(this), null, false);
                } catch (IOException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
                }

                break;
            }
            default:
                commands.doOther();
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
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
