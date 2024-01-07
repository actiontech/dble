/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.statistic.sql.StatisticListener;

public final class BeginHandler {
    private BeginHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        service.getSession2().endParseTCL();
        if (service.isTxStart() || !service.isAutocommit()) {
            service.beginInTx(stmt);
        } else {
            service.setTxStart(true);
            StatisticListener.getInstance().record(service, r -> r.onTxStart(service));
            TxnLogHelper.putTxnLog(service, stmt);
            service.writeOkPacket();
            service.getSession2().setResponseTime(true);
        }
    }
}
