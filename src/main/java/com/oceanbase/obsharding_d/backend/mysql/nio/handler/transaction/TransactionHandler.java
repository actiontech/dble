/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction;

import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;

import java.sql.SQLException;

public interface TransactionHandler {

    void commit();

    void commit(TransactionCallback transactionCallback);

    void syncImplicitCommit() throws SQLException;

    void rollback();

    void rollback(TransactionCallback transactionCallback);

    void turnOnAutoCommit(MySQLPacket previousSendData);
}
