/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ShowVariablesHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

public final class ShowVariables {
    private ShowVariables() {
    }

    public static void response(ShardingService shardingService, String stmt) {
        String db = shardingService.getSchema() != null ? shardingService.getSchema() : SchemaUtil.getRandomDb();

        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            shardingService.writeErrMessage("42000", "Unknown database '" + db + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        RouteResultset rrs = new RouteResultset(stmt, ServerParse.SHOW);
        try {
            RouterUtil.routeToSingleNode(rrs, schema.getRandomShardingNode());
            ShowVariablesHandler handler = new ShowVariablesHandler(rrs, shardingService.getSession2());
            try {
                handler.execute();
            } catch (Exception e1) {
                handler.recycleBuffer();
                shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e1.toString());
            }
        } catch (Exception e) {
            // Could this only be ER_PARSE_ERROR?
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
