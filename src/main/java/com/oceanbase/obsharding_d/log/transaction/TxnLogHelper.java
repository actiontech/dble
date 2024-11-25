/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.transaction;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.SystemConfig;

import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

public final class TxnLogHelper {
    private TxnLogHelper() {
    }

    public static void putTxnLog(ShardingService service, String sql) {
        if (SystemConfig.getInstance().getRecordTxn() == 1) {
            OBsharding_DServer.getInstance().getTxnLogProcessor().putTxnLog(service, sql);
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
                OBsharding_DServer.getInstance().getTxnLogProcessor().putTxnLog(service, sb.toString());
        }
    }

    // single-node
    public static void putTxnLog(ShardingService service, final RouteResultsetNode node) {
        if (SystemConfig.getInstance().getRecordTxn() == 1 && node.isModifySQL()) {
            OBsharding_DServer.getInstance().getTxnLogProcessor().putTxnLog(service, "[" + node.getName() + "]" + node.getStatement());
        }
    }
}
