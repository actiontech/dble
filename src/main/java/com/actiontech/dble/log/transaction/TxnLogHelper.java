/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.transaction;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;

import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

public final class TxnLogHelper {
    private TxnLogHelper() {
    }

    public static void putTxnLog(ShardingService service, String sql) {
        if (SystemConfig.getInstance().getRecordTxn() == 1) {
            DbleServer.getInstance().getTxnLogProcessor().putTxnLog(service, sql);
        }
    }

    // multi-node
    public static void putTxnLog(ShardingService service, final RouteResultset rrs) {
        if (SystemConfig.getInstance().getRecordTxn() == 1) {
            StringBuilder sb = new StringBuilder();
            for (final RouteResultsetNode node : rrs.getNodes()) {
                if (node.isModifySQL())
                    sb.append("[").append(node.getName()).append("]").append(node.getStatement()).append(";\n");
            }
            if (sb.length() > 0)
                DbleServer.getInstance().getTxnLogProcessor().putTxnLog(service, sb.toString());
        }
    }

    // single-node
    public static void putTxnLog(ShardingService service, final RouteResultsetNode node) {
        if (SystemConfig.getInstance().getRecordTxn() == 1 &&
                service.isInTransaction() && node.isModifySQL()) {
            DbleServer.getInstance().getTxnLogProcessor().putTxnLog(service, "[" + node.getName() + "]" + node.getStatement());
        }
    }
}
