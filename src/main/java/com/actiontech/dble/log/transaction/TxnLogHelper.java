/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.transaction;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;

import com.actiontech.dble.services.mysqlsharding.ShardingService;

public final class TxnLogHelper {
    private TxnLogHelper() {
    }

    public static void putTxnLog(ShardingService service, String sql) {
        if (SystemConfig.getInstance().getRecordTxn() == 1) {
            DbleServer.getInstance().getTxnLogProcessor().putTxnLog(service, sql);
        }
    }
}
