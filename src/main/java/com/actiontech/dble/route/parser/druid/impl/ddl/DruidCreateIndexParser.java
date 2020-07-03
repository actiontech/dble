/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.route.util.RouterUtil;

import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidCreateIndexParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
                                     ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        rrs.setOnline(true);
        SQLCreateIndexStatement createStmt = (SQLCreateIndexStatement) stmt;
        SQLTableSource tableSource = createStmt.getTable();
        if (tableSource instanceof SQLExprTableSource) {
            String schemaName = schema == null ? null : schema.getName();
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, (SQLExprTableSource) tableSource);
            String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
            rrs.setStatement(statement);
            String noShardingNode = RouterUtil.isNoShardingDDL(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            if (noShardingNode != null) {
                RouterUtil.routeToSingleDDLNode(schemaInfo, rrs, noShardingNode);
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
