package com.actiontech.dble.services.manager;

import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.FrontEndService;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.CommandCount;

import java.io.UnsupportedEncodingException;

/**
 * Created by szf on 2020/6/28.
 */
public class ManagerService extends FrontEndService {

    private final ManagerQueryHandler handler;

    protected UserName user;

    private volatile String executeSql;
    private volatile String schema;

    private final ManagerSession session;

    protected final CommandCount commands;

    public ManagerService(AbstractConnection connection) {
        super(connection);
        this.handler = new ManagerQueryHandler(this);
        this.proto = new MySQLProtoHandlerImpl();
        this.session = new ManagerSession(this);
        this.commands = connection.getProcessor().getCommands();
    }

    public void initFromAuthInfo(AuthResultInfo info) {
        AuthPacket auth = info.getMysqlAuthPacket();
        this.user = new UserName(auth.getUser(), auth.getTenant());
        this.schema = info.getMysqlAuthPacket().getDatabase();
        this.userConfig = info.getUserConfig();
        this.handler.setReadOnly(((ManagerUserConfig) userConfig).isReadOnly());
        connection.initCharsetIndex(info.getMysqlAuthPacket().getCharsetIndex());
        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            this.setSupportCompress(true);
        }
    }


    @Override
    protected void handleInnerData(byte[] data) {
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                this.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                try {
                    handler.query(getCommand(data, this.getConnection().getCharsetName()));
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + this.getConnection().getCharsetName().getClient() + "'");
                }
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                PingPacket.response(this);
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                connection.close("quit cmd");
                break;
            default:
                commands.doOther();
                this.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public ManagerUserConfig getUserConfig() {
        return (ManagerUserConfig) userConfig;
    }

    @Override
    public void userConnectionCount() {
        FrontendUserManager.getInstance().countDown(user, true);
    }

    @Override
    public UserName getUser() {
        return user;
    }

    @Override
    public String getExecuteSql() {
        return executeSql;
    }

    @Override
    public void killAndClose(String reason) {
        connection.close(reason);
    }


    public String getCommand(byte[] data, CharsetNames charsetName) throws UnsupportedEncodingException {
        String sql = null;
        try {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            sql = mm.readString(charsetName.getClient());
            executeSql = sql;
        } catch (UnsupportedEncodingException e) {
            throw e;
        }
        return sql;
    }

    public String toBriefString() {
        return "managerService";
    }

    protected void sessionStart() {
        TraceManager.sessionStart(this, "manager-server-start");
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public FrontendConnection getConnection() {
        return (FrontendConnection) connection;
    }

    public ManagerSession getSession2() {
        return session;
    }

    public void initDB(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String db = null;
        try {
            db = mm.readString(this.getCharset().getClient());
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + this.getCharset().getClient() + "'");
            return;
        }
        if (db != null) {
            db = db.toLowerCase();
        }
        // check sharding
        if (db == null || !ManagerSchemaInfo.SCHEMA_NAME.equals(db)) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }
        this.schema = db;
        OkPacket okPacket = new OkPacket();
        okPacket.read(OkPacket.OK);
        okPacket.write(this.getConnection());
    }

    @Override
    public String toString() {
        return "ManagerService [user = " + user + " sql = " + executeSql + " schema = " + schema + " ] \nWith connection " + connection.toString();
    }

}
