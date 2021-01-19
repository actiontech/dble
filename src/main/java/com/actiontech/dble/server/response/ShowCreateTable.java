/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.util.RouterUtil;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public final class ShowCreateTable {
    private ShowCreateTable() {
    }

    public static void response(ShardingService shardingService, String stmt) {
        try {
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            SQLShowCreateTableStatement showCreateTableStatement = (SQLShowCreateTableStatement) statement;
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(shardingService.getUser(), shardingService.getSchema(), showCreateTableStatement.getName(), null);
            shardingService.routeSystemInfoAndExecuteSQL(RouterUtil.removeSchema(stmt, schemaInfo.getSchema()), schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
