/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.response.FieldList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Created by szf on 2020/7/2.
 */
public class MySQLProtoLogicHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtoLogicHandler.class);

    private final ShardingService service;

    private volatile byte[] multiQueryData = null;

    MySQLProtoLogicHandler(ShardingService service) {
        this.service = service;
    }


    public void initDB(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String db = null;
        try {
            db = mm.readString(service.getCharset().getClient());
        } catch (UnsupportedEncodingException e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + service.getCharset().getClient() + "'");
            return;
        }
        if (db != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            db = db.toLowerCase();
        }
        // check sharding
        if (db == null || !DbleServer.getInstance().getConfig().getSchemas().containsKey(db)) {
            service.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }
        if (!service.getUserConfig().getSchemas().contains(db)) {
            String s = "Access denied for user '" + service.getUser() + "' to database '" + db + "'";
            service.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
            return;
        }
        service.setSchema(db);
        service.getSession2().setRowCount(0);
        service.write(OkPacket.getDefault());
    }


    public void query(byte[] data) {
        this.multiQueryData = data;
        String sql = null;
        try {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            sql = mm.readString(service.getCharset().getClient());
        } catch (UnsupportedEncodingException e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + service.getCharset().getClient() + "'");
            return;
        }
        service.query(sql);
    }

    public String stmtPrepare(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String sql = null;
        try {
            sql = mm.readString(service.getCharset().getClient());
        } catch (UnsupportedEncodingException e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
                    "Unknown charset '" + service.getCharset().getClient() + "'");
            return null;
        }
        if (sql == null || sql.length() == 0) {
            service.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return null;
        }
        return sql;
    }

    public void fieldList(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        FieldList.response(service, mm.readStringWithNull());
    }

    public byte[] getMultiQueryData() {
        return multiQueryData;
    }
}
