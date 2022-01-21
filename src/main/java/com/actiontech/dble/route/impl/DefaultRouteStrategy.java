/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.impl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidParserFactory;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.DruidUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;

public class DefaultRouteStrategy extends AbstractRouteStrategy {

    @Override
    protected RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String originSql, RouteResultset rrs,
                                                   ShardingService service, boolean isExplain) throws SQLException {
        SQLStatement statement = DruidUtil.parseSQL(originSql);
        if (service.getSession2().getIsMultiStatement().get()) {
            originSql = statement.toString();
            rrs.setStatement(originSql);
            rrs.setSrcStatement(originSql);
        }
        DruidParser druidParser = DruidParserFactory.create(statement, rrs.getSqlType(), service);
        String schemaName = schema == null ? null : schema.getName();
        return RouterUtil.routeFromParser(druidParser, schema, rrs, statement, new ServerSchemaStatVisitor(schemaName), service, isExplain);
    }

}
