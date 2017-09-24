/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.server.ServerConnection;

public final class RollBackHandler {
    private RollBackHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        TxnLogHelper.putTxnLog(c, stmt);
        c.rollback();
    }
}
