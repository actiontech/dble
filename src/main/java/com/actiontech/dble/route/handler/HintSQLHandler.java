/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.RouteStrategy;
import com.actiontech.dble.route.factory.RouteStrategyFactory;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Map;

/**
 * HintSQLHandler
 */
public class HintSQLHandler implements HintHandler {

    private RouteStrategy routeStrategy;

    HintSQLHandler() {
        this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ShardingService service,
                                String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {

        RouteResultset rrs = routeStrategy.route(schema, hintSqlType,
                hintSQLValue, service);

        if (rrs.isNeedOptimizer()) {
            throw new SQLSyntaxErrorException("Complex SQL not supported in hint");
        }
        // replace the sql of RRS
        if (ServerParse.CALL == sqlType) {
            rrs.setCallStatement(true);
        }

        RouteResultsetNode[] oldRsNodes = rrs.getNodes();
        RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
        for (int i = 0; i < newRrsNodes.length; i++) {
            newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(), sqlType, realSQL);
        }
        rrs.setNodes(newRrsNodes);

        return rrs;
    }

}
