/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.net.mysql.MySQLPacket;

public interface TransactionHandler {
    void commit();

    void implicitCommit(ImplicitCommitHandler handler);

    void rollback();

    void turnOnAutoCommit(MySQLPacket previousSendData);
}
