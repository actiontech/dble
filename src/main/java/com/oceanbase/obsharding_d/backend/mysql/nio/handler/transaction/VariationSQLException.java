/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction;

import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;

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
