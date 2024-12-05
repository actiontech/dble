/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.FieldListHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.google.common.collect.Sets;

public final class FieldList {
    private static final String SQL_SELECT_TABLE = "SELECT * FROM {0} LIMIT 0;";

    private FieldList() {
    }

    public static void response(ShardingService service, String table) {
        String cSchema = service.getSchema();
        if (cSchema == null) {
            service.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }

        SchemaConfig schemaConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schemaConfig == null) {
            service.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (OBsharding_DServer.getInstance().getConfig().getUsers().get(service.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            service.writeErrMessage("42000", "Access denied for user '" + service.getUser().getFullName() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        String shardingNode;
        if (!schemaConfig.getTables().containsKey(table)) {
            if ((shardingNode = schemaConfig.getDefaultSingleNode()) == null) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + cSchema + "." + table + "] doesn't exist");
                return;
            }
        } else {
            BaseTableConfig tableConfig = schemaConfig.getTables().get(table);
            if (tableConfig == null) {
                service.writeErrMessage(ErrorCode.ER_YES, "The table " + table + " doesn't exist");
                return;
            }
            shardingNode = tableConfig.getShardingNodes().get(0);
        }

        try {
            String sql = SQL_SELECT_TABLE.replace("{0}", table);
            RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
            rrs.setSchema(cSchema);
            RouterUtil.routeToSingleNode(rrs, shardingNode, Sets.newHashSet(cSchema + "." + table));
            FieldListHandler fieldsListHandler = new FieldListHandler(service.getSession2(), rrs);
            fieldsListHandler.execute();
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
