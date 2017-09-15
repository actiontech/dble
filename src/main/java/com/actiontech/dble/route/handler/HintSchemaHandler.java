/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteStrategy;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * HintSchemaHandler
 */
public class HintSchemaHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

    private RouteStrategy routeStrategy;

    public HintSchemaHandler() {
        this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    /**
     * @param schema
     * @param sqlType
     * @param realSQL
     * @param sc
     * @param cachePool
     * @param hintSQLValue
     * @return
     * @throws SQLNonTransientException
     */
    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {
        SchemaConfig tempSchema = DbleServer.getInstance().getConfig().getSchemas().get(hintSQLValue);
        if (tempSchema != null) {
            return routeStrategy.route(tempSchema, sqlType, realSQL, sc, cachePool);
        } else {
            String msg = "can't find hint schema:" + hintSQLValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
    }
}
