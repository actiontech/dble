package io.mycat.server.handler;

import io.mycat.log.transaction.TxnLogHelper;
import io.mycat.server.ServerConnection;

public final class RollBackHandler {
    private RollBackHandler() {
    }
    public static void handle(String stmt, ServerConnection c) {
        TxnLogHelper.putTxnLog(c, stmt);
        c.rollback();
    }
}
