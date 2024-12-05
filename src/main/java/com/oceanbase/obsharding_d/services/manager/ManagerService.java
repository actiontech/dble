/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager;

import com.oceanbase.obsharding_d.backend.mysql.MySQLMessage;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.user.ManagerUserConfig;
import com.oceanbase.obsharding_d.log.general.GeneralLogHelper;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.CharsetNames;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.net.mysql.PingPacket;
import com.oceanbase.obsharding_d.net.service.AuthResultInfo;
import com.oceanbase.obsharding_d.net.service.ServiceTask;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.CommandCount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;

/**
 * Created by szf on 2020/6/28.
 */
public class ManagerService extends FrontendService<ManagerUserConfig> {
    private static final Logger LOGGER = LogManager.getLogger(ManagerService.class);

    private final ManagerQueryHandler handler;
    private final ManagerSession session;

    protected final CommandCount commands;


    public ManagerService(AbstractConnection connection, AuthResultInfo info) {
        super(connection, info);
        this.handler = new ManagerQueryHandler(this);
        this.handler.setReadOnly(userConfig.isReadOnly());
        this.session = new ManagerSession(this);
        this.commands = connection.getProcessor().getCommands();
    }

    @Override
    protected void handleInnerData(byte[] data) {
        GeneralLogHelper.putGLog(this, data);
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                this.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                try {
                    handler.query(getCommand(data, charsetName));
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charsetName.getClient() + "'");
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

    @Override
    protected boolean beforeHandlingTask(@NotNull ServiceTask task) {
        TraceManager.sessionStart(this, "manager-server-start");
        return true;
    }


    public void killAndClose(String reason) {
        connection.close(reason);
    }

    private String getCommand(byte[] data, CharsetNames charsetName) throws UnsupportedEncodingException {
        String sql = null;
        try {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            sql = mm.readString(charsetName.getClient());
            // remove last ';'
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            executeSql = sql;
        } catch (UnsupportedEncodingException e) {
            throw e;
        }
        return sql;
    }

    public String toBriefString() {
        return "managerService";
    }

    @Override
    public FrontendConnection getConnection() {
        return (FrontendConnection) connection;
    }

    public ManagerSession getSession2() {
        return session;
    }

    private void initDB(byte[] data) {
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
        OkPacket okPacket = OkPacket.getDefault();
        okPacket.write(this.getConnection());
    }

    @Override
    public String toString() {
        return "ManagerService [" + user + " sql = " + executeSql + " schema = " + schema + " ] with " + connection.toString();
    }

}
