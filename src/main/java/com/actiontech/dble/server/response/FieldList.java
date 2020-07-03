package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.FieldListHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

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

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schemaConfig == null) {
            service.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(service.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            service.writeErrMessage("42000", "Access denied for user '" + service.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        String shardingNode;
        if (!schemaConfig.getTables().containsKey(table)) {
            if ((shardingNode = schemaConfig.getShardingNode()) == null) {
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
            RouterUtil.routeToSingleNode(rrs, shardingNode);
            FieldListHandler fieldsListHandler = new FieldListHandler(service.getSession2(), rrs);
            fieldsListHandler.execute();
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
