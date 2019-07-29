/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ShowVariablesHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;

public final class ShowVariables {
    private ShowVariables() {
    }

    public static void response(ServerConnection c, String stmt) {
        String db = c.getSchema() != null ? c.getSchema() : SchemaUtil.getRandomDb();

        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            c.writeErrMessage("42000", "Unknown database '" + db + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        RouteResultset rrs = new RouteResultset(stmt, ServerParse.SHOW);
        try {
            RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode());
            ShowVariablesHandler handler = new ShowVariablesHandler(rrs, c.getSession2());
            handler.execute();
        } catch (Exception e) {
            // Could this only be ER_PARSE_ERROR?
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
