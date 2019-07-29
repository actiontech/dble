/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.server.ServerConnection;

public final class BeginHandler {
    private BeginHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        if (c.isTxStart() || !c.isAutocommit()) {
            c.beginInTx(stmt);
        } else {
            c.setTxStart(true);
            TxnLogHelper.putTxnLog(c, stmt);
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            c.write(c.writeToBuffer(c.getSession2().getOkByteArray(), c.allocate()));
            c.getSession2().multiStatementNextSql(multiStatementFlag);
        }
    }
}
