/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl.ddl;

import com.oceanbase.obsharding_d.cluster.values.DDLInfo;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidImplicitCommitParser;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;

import java.sql.SQLException;

public class DruidTruncateTableParser extends DruidImplicitCommitParser {
    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        rrs.setDdlType(DDLInfo.DDLType.TRUNCATE_TABLE);
        String schemaName = schema == null ? null : schema.getName();
        SQLTruncateStatement truncateTable = (SQLTruncateStatement) stmt;
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, truncateTable.getTableSources().get(0));
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        if (RouterUtil.tryRouteToSingleDDLNode(schemaInfo, rrs, schemaInfo.getTable())) {
            return schemaInfo.getSchemaConfig();
        }
        RouterUtil.routeToDDLNode(schemaInfo, rrs);
        return schemaInfo.getSchemaConfig();
    }
}
