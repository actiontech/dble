/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;

public class ShardingStatisticRecord extends StatisticRecord {

    public void onBackendSqlStart(MySQLResponseService service) {
        if (frontendSqlEntry != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            BackendConnection connection = service.getConnection();
            ShardingService shardingService = service.getSession().getShardingService();

            StatisticBackendSqlEntry entry = new StatisticBackendSqlEntry(
                    frontendInfo,
                    ((MySQLInstance) connection.getInstance()).getName(), connection.getHost(), connection.getPort(), node.getName(),
                    node.getSqlType(), node.getStatement(), System.nanoTime());

            if (txEntry != null) {
                entry.setTxId(shardingService.getTxId());
            }
            String key = connection.getId() + ":" + node.getName() + ":" + node.getStatementHash();
            frontendSqlEntry.put(key, entry);
        }
    }

    public ShardingStatisticRecord(BusinessService service) {
        super(service);
    }
}
