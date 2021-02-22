/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.statistic.sql.StatisticListener;

import java.util.Optional;

public final class RollBackHandler {
    private RollBackHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        if (service.isTxStart() || !service.isAutocommit()) {
            Optional.ofNullable(StatisticListener.getInstance().getRecorder(service)).ifPresent(r -> r.onTxEndByRollback());
            TxnLogHelper.putTxnLog(service, stmt);
        }
        service.transactionsCount();
        service.rollback();
    }
}
