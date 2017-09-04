package com.actiontech.dble.log.transaction;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.server.ServerConnection;

public final class TxnLogHelper {
    private TxnLogHelper() {
    }

    public static void putTxnLog(ServerConnection c, String sql) {
        if (DbleServer.getInstance().getConfig().getSystem().getRecordTxn() == 1) {
            DbleServer.getInstance().getTxnLogProcessor().putTxnLog(c, sql);
        }
    }
}
