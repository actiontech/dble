/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql;

import com.oceanbase.obsharding_d.backend.mysql.nio.MySQLInstance;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.services.BusinessService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticBackendSqlEntry;

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
