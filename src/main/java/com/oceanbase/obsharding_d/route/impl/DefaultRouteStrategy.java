/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.impl;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.DruidParser;
import com.oceanbase.obsharding_d.route.parser.druid.DruidParserFactory;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
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
