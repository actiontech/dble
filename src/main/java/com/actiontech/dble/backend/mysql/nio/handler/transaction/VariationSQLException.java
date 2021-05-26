package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.net.mysql.MySQLPacket;

import java.sql.SQLException;

public class VariationSQLException extends SQLException {

    private final MySQLPacket sendData;

    public VariationSQLException(MySQLPacket sendData) {
        super("VariationSQLException");
        this.sendData = sendData;
    }

    public MySQLPacket getSendData() {
        return sendData;
    }
}
