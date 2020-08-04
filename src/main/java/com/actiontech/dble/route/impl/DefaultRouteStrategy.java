/*
 * Copyright (C) 2016-2020 ActionTech.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

public class DefaultRouteStrategy extends AbstractRouteStrategy {

    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultRouteStrategy.class);


    public SQLStatement parserSQL(String originSql, ShardingService service) throws SQLSyntaxErrorException {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "sql-parse");
        try {
            SQLStatementParser parser;
            parser = new MySqlStatementParser(originSql);
            try {
                return parser.parseStatement(true);
            } catch (Exception t) {
                LOGGER.info("routeNormalSqlWithAST", t);
                if (t.getMessage() != null) {
                    throw new SQLSyntaxErrorException(t.getMessage());
                } else {
                    throw new SQLSyntaxErrorException(t);
                }
            }
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }
    }

    @Override
    public SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(originSql);

        /**
         * thrown SQL SyntaxError if parser error
         */
        try {
            List<SQLStatement> list = parser.parseStatementList();
            if (list.size() > 1) {
                throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
            }
            return list.get(0);
        } catch (Exception t) {
            LOGGER.info("routeNormalSqlWithAST", t);
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ShardingService service) throws SQLException {
        return this.route(schema, sqlType, origSQL, service, false);
    }


    @Override
    public RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
                                                String originSql, RouteResultset rrs,
                                                ShardingService service, boolean isExplain) throws SQLException {
        SQLStatement statement = parserSQL(originSql, service);
        if (service.getSession2().getIsMultiStatement().get()) {
            originSql = statement.toString();
            rrs.setStatement(originSql);
            rrs.setSrcStatement(originSql);
        }
        service.getSession2().endParse();
        DruidParser druidParser = DruidParserFactory.create(statement, rrs.getSqlType());
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "simple-route-detail");
        TraceManager.log(ImmutableMap.of("druidParser", druidParser.getClass().toString()), traceObject);
        try {
            return RouterUtil.routeFromParser(druidParser, schema, rrs, statement, new ServerSchemaStatVisitor(), service, isExplain);
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }

    }

}
