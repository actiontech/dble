/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl.ddl;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidImplicitCommitParser;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidCreateIndexParser extends DruidImplicitCommitParser {
    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        rrs.setOnline(true);
        SQLCreateIndexStatement createStmt = (SQLCreateIndexStatement) stmt;
        SQLTableSource tableSource = createStmt.getTable();
        if (tableSource instanceof SQLExprTableSource) {
            String schemaName = schema == null ? null : schema.getName();
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, (SQLExprTableSource) tableSource);
            String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
            rrs.setStatement(statement);
            if (RouterUtil.tryRouteToSingleDDLNode(schemaInfo, rrs, schemaInfo.getTable())) {
                return schemaInfo.getSchemaConfig();
            }
            RouterUtil.routeToDDLNode(schemaInfo, rrs);
            return schemaInfo.getSchemaConfig();
        } else {
            String msg = "The DDL is not supported, sql:" + stmt;
            throw new SQLNonTransientException(msg);
        }
    }
}
