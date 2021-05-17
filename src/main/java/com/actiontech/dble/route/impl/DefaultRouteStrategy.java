/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.impl;

import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidParserFactory;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

public class DefaultRouteStrategy extends AbstractRouteStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRouteStrategy.class);

    @Override
    protected RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
                                                   String originSql, RouteResultset rrs,
                                                   LayerCachePool cachePool, ServerConnection sc) throws SQLException {
        SQLStatement statement = parse(originSql);
        if (sc.getSession2().getIsMultiStatement().get()) {
            originSql = statement.toString();
            rrs.setStatement(originSql);
            rrs.setSrcStatement(originSql);
        }
        DruidParser druidParser = DruidParserFactory.create(statement, rrs.getSqlType());
        return RouterUtil.routeFromParser(druidParser, schema, rrs, statement, originSql, cachePool, new ServerSchemaStatVisitor(), sc, null);
    }

    private SQLStatement parse(String originSql) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(originSql);
        try {
            return parser.parseStatement(true);
        } catch (Exception t) {
            LOGGER.warn("routeNormalSqlWithAST", t);
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
    }

}
