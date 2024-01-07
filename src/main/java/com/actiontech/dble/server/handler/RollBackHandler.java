/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.statistic.sql.StatisticListener;

public final class RollBackHandler {
    private RollBackHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        service.getSession2().endParseTCL();
        if (service.isTxStart() || !service.isAutocommit()) {
            StatisticListener.getInstance().record(service, r -> r.onTxEnd());
            TxnLogHelper.putTxnLog(service, stmt);
        }
        service.transactionsCount();
        service.rollback();
    }
}
