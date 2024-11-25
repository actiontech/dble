/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ShowVariablesHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.util.RouterUtil;

import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

public final class ShowVariables {
    private ShowVariables() {
    }

    public static void response(ShardingService shardingService, String stmt) {
        String db = shardingService.getSchema() != null ? shardingService.getSchema() : SchemaUtil.getRandomDb();

        SchemaConfig schema = OBsharding_DServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            shardingService.writeErrMessage("42000", "Unknown database '" + db + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        RouteResultset rrs = new RouteResultset(stmt, ServerParse.SHOW);
        try {
            RouterUtil.routeToSingleNode(rrs, schema.getRandomShardingNode(), null);
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
