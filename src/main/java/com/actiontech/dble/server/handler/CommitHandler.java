/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

public final class CommitHandler {
    private CommitHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        if (service.isTxStart() || !service.isAutocommit()) {
            TxnLogHelper.putTxnLog(service, stmt);
        }
        service.transactionsCount();
        service.commit(stmt);
    }
}
