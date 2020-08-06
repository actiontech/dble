/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.util.RouterUtil;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;

public final class DescribeHandler {
    private DescribeHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        try {
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            MySqlExplainStatement describeStatement = (MySqlExplainStatement) statement;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), service.getSchema(), describeStatement.getTableName(), null);
            service.routeSystemInfoAndExecuteSQL(RouterUtil.removeSchema(stmt, schemaInfo.getSchema()), schemaInfo, ServerParse.DESCRIBE);
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
            return;
        }
    }
}
