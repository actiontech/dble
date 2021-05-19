/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.impl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteStrategy;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.mpp.LoadData;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

public abstract class AbstractRouteStrategy implements RouteStrategy {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

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
            LOGGER.warn("routeNormalSqlWithAST", t);
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL,
                                ShardingService service, boolean isExplain) throws SQLException {

        RouteResultset rrs = new RouteResultset(origSQL, sqlType);

        /*
         * debug mode and load data ,no cache
         */
        if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.LOAD_DATA_HINT)) {
            rrs.setSqlRouteCacheAble(false);
        }

        if (sqlType == ServerParse.CALL) {
            rrs.setCallStatement(true);
        }

        if (schema == null) {
            rrs = routeNormalSqlWithAST(null, origSQL, rrs, service, isExplain);
        } else {
            if (sqlType == ServerParse.SHOW) {
                rrs.setStatement(origSQL);
                rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomShardingNode());
            } else {
                rrs = routeNormalSqlWithAST(schema, origSQL, rrs, service, isExplain);
            }
        }

        service.getSession2().endParse();
        return rrs;
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ShardingService service) throws SQLException {
        return this.route(schema, sqlType, origSQL, service, false);
    }

    /**
     * routeNormalSqlWithAST
     */
    protected abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs,
                                                            ShardingService service, boolean isExplain) throws SQLException;

}
