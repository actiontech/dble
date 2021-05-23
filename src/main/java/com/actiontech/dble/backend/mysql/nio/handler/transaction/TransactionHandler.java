/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.net.mysql.MySQLPacket;

import java.sql.SQLException;

public interface TransactionHandler {

    void commit();

    void implicitCommit(ImplicitCommitHandler handler);

    void syncImplicitCommit() throws SQLException;

    void rollback();

    void turnOnAutoCommit(MySQLPacket previousSendData);
}
