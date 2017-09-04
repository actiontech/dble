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
