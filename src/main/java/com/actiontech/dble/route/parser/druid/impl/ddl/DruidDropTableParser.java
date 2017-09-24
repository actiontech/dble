/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidDropTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        SQLDropTableStatement dropTable = (SQLDropTableStatement) stmt;
        if (dropTable.getTableSources().size() > 1) {
            String msg = "dropping multi-tables is not supported, sql:" + stmt;
            throw new SQLNonTransientException(msg);
        }
        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, dropTable.getTableSources().get(0));
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        if (RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable())) {
            RouterUtil.routeToSingleDDLNode(schemaInfo, rrs);
            return schemaInfo.getSchemaConfig();
        }
        RouterUtil.routeToDDLNode(schemaInfo, rrs);
        return schemaInfo.getSchemaConfig();
    }
}
