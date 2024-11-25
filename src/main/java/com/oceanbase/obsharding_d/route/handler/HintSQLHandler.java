/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.handler;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.factory.RouteStrategyFactory;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

/**
 * HintSQLHandler
 */
public final class HintSQLHandler {

    private HintSQLHandler() {
    }

    public static RouteResultset route(SchemaConfig schema, String hintSQL, int realSqlType, String realSQL, ShardingService service)
            throws SQLException {

        ServerParse serverParse = ServerParseFactory.getShardingParser();
        int hintSqlType = serverParse.parse(hintSQL) & 0xff;
        RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(schema, hintSqlType, hintSQL, service);

        if (rrs.isNeedOptimizer()) {
            throw new SQLSyntaxErrorException("Complex SQL not supported in hint");
        }
        // replace the sql of RRS
        if (ServerParse.CALL == realSqlType) {
            rrs.setCallStatement(true);
        }

        RouteResultsetNode[] oldRsNodes = rrs.getNodes();
        RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
        for (int i = 0; i < newRrsNodes.length; i++) {
            newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(), realSqlType, realSQL);
        }
        rrs.setNodes(newRrsNodes);
        // HintSQLHandler will always send to master
        rrs.setRunOnSlave(false);
        return rrs;
    }

}
