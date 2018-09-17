/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.*;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Map;

/**
 * HintSQLHandler
 */
public class HintSQLHandler implements HintHandler {

    private RouteStrategy routeStrategy;

    public HintSQLHandler() {
        this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {

        RouteResultset rrs = routeStrategy.route(schema, hintSqlType,
                hintSQLValue, sc, cachePool);

        if (rrs.isNeedOptimizer()) {
            throw new SQLSyntaxErrorException("Complex SQL not supported in hint");
        }
        // replace the sql of RRS
        RouteResultsetNode[] oldRsNodes = rrs.getNodes();
        RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
        for (int i = 0; i < newRrsNodes.length; i++) {
            newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(),
                    oldRsNodes[i].getSqlType(), realSQL);
        }
        rrs.setNodes(newRrsNodes);

        return rrs;
    }

}
