/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.impl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidParserFactory;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

public class DefaultRouteStrategy extends AbstractRouteStrategy {

    @Override
    protected RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String originSql, RouteResultset rrs,
                                                   ShardingService service, boolean isExplain) throws SQLException {
        SQLStatement statement = parserSQL(originSql, service);
        if (service.getSession2().getIsMultiStatement().get()) {
            originSql = statement.toString();
            rrs.setStatement(originSql);
            rrs.setSrcStatement(originSql);
        }
        DruidParser druidParser = DruidParserFactory.create(statement, rrs.getSqlType());
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "simple-route-detail");
        TraceManager.log(ImmutableMap.of("druidParser", druidParser.getClass().toString()), traceObject);
        try {
            String schemaName = schema == null ? null : schema.getName();
            return RouterUtil.routeFromParser(druidParser, schema, rrs, statement, new ServerSchemaStatVisitor(schemaName), service, isExplain);
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }
    }

    private SQLStatement parserSQL(String originSql, ShardingService service) throws SQLSyntaxErrorException {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "sql-parse");
        try {
            SQLStatementParser parser;
            parser = new MySqlStatementParser(originSql);
            try {
                return parser.parseStatement(true);
            } catch (Exception t) {
                LOGGER.warn("routeNormalSqlWithAST", t);
                if (t.getMessage() != null) {
                    throw new SQLSyntaxErrorException("druid not support sql syntax, the reason is " + t.getMessage());
                } else {
                    throw new SQLSyntaxErrorException("druid not support sql syntax, the reason is " + t);
                }
            }
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }
    }

}
