/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidImplicitCommitParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map;

public class DruidDropTableParser extends DruidImplicitCommitParser {
    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        SQLDropTableStatement dropTable = (SQLDropTableStatement) stmt;
        rrs.setDdlType(DDLInfo.DDLType.DROP_TABLE);
        if (dropTable.getTableSources().size() > 1) {
            String msg = "dropping multi-tables is not supported, sql:" + stmt;
            throw new SQLNonTransientException(msg);
        }
        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, dropTable.getTableSources().get(0));
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        if (RouterUtil.tryRouteToSingleDDLNode(schemaInfo, rrs, schemaInfo.getTable())) {
            return schemaInfo.getSchemaConfig();
        }
        Map<String, BaseTableConfig> tables = schemaInfo.getSchemaConfig().getTables();
        BaseTableConfig tc = tables.get(schemaInfo.getTable());
        if (tc == null) {
            if (dropTable.isIfExists()) {
                rrs.setFinishedExecute(true);
            } else {
                String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
        } else {
            RouterUtil.routeToDDLNode(schemaInfo, rrs);
        }
        return schemaInfo.getSchemaConfig();
    }
}
