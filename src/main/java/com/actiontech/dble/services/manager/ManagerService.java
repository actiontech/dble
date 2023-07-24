/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.log.general.GeneralLogHelper;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.PingPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.NotificationServiceTask;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.CommandCount;
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
    public void handle(ServiceTask task) {
        beforeInsertServiceTask(task);
        task.setTaskId(taskId.getAndIncrement());
        DbleServer.getInstance().getManagerFrontHandlerQueue().offer(task);
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

    public void notifyTaskThread() {
        DbleServer.getInstance().getManagerFrontHandlerQueue().offerFirst(new NotificationServiceTask(this));
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
