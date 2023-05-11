/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.singleton;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.handler.HintMasterDBHandler;
import com.actiontech.dble.route.handler.HintPlanHandler;
import com.actiontech.dble.route.handler.HintSQLHandler;
import com.actiontech.dble.route.handler.HintShardingNodeHandler;
import com.actiontech.dble.route.parser.DbleHintParser;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public final class RouteService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteService.class);

    private static final RouteService INSTANCE = new RouteService();

    private RouteService() {
    }

    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String stmt, ShardingService service) throws SQLException {
        return this.route(schema, sqlType, stmt, service, false);
    }

    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String stmt, ShardingService service, boolean isExplain)
            throws SQLException {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "simple-route");
        RouteResultset rrs = null;
        try {
            String cacheKey = null;

            if (sqlType == ServerParse.SELECT && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null) {
                cacheKey = (schema == null ? "NULL" : schema.getName()) + "_" + service.getUser().getFullName() + "_" + stmt;
                rrs = (RouteResultset) CacheService.getSqlRouteCache().get(cacheKey);
                if (rrs != null) {
                    service.getSession2().trace(t -> t.endParse());
                    return rrs;
                }
            }

            DbleHintParser.HintInfo hintInfo = DbleHintParser.parse(stmt);
            if (hintInfo == null) {
                stmt = stmt.trim();
                rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, stmt, service, isExplain);
            } else {
                int type = hintInfo.getType();
                if (type == DbleHintParser.SQL) {
                    rrs = HintSQLHandler.route(schema, hintInfo.getHintValue(), sqlType, hintInfo.getRealSql(), service);
                } else if (type == DbleHintParser.SHARDING_NODE) {
                    rrs = HintShardingNodeHandler.route(hintInfo.getHintValue(), sqlType, hintInfo.getRealSql(), service);
                } else if (type == DbleHintParser.PLAN) {
                    rrs = HintPlanHandler.route(hintInfo.getHintValue(), sqlType, hintInfo.getRealSql());
                } else if (type == DbleHintParser.DB_TYPE) {
                    rrs = HintMasterDBHandler.route(schema, hintInfo.getHintValue(), sqlType, hintInfo.getRealSql(), service);
                } else {
                    throw new SQLException("current hint type is not supported");
                }
            }

            if (rrs != null && sqlType == ServerParse.SELECT && rrs.isSqlRouteCacheAble() && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null &&
                    service.getSession2().getRemainingSql() == null) {
                CacheService.getSqlRouteCache().putIfAbsent(cacheKey, rrs);
            }
            return rrs;
        } finally {
            if (rrs != null) {
                TraceManager.log(ImmutableMap.of("route-result-set", rrs), traceObject);
            }
            TraceManager.finishSpan(service, traceObject);
        }
    }

    public static RouteService getInstance() {
        return INSTANCE;
    }

}
