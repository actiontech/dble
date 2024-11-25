/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
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
            SQLStatement statement = DruidUtil.parseMultiSQL(stmt);
            SQLShowCreateTableStatement showCreateTableStatement = (SQLShowCreateTableStatement) statement;
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(shardingService.getUser(), shardingService.getSchema(), showCreateTableStatement.getName(), null);
            shardingService.routeSystemInfoAndExecuteSQL(RouterUtil.removeSchema(stmt, schemaInfo.getSchema()), schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
