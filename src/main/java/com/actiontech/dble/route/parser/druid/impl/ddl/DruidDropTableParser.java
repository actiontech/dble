/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.net.mysql.OkPacket;
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
import java.util.Map;

public class DruidDropTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        SQLDropTableStatement dropTable = (SQLDropTableStatement) stmt;
        rrs.setDdlType(DDLInfo.DDLType.DROP_TABLE);
        if (dropTable.getTableSources().size() > 1) {
            String msg = "dropping multi-tables is not supported, sql:" + stmt;
            throw new SQLNonTransientException(msg);
        }
        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, dropTable.getTableSources().get(0));
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        String noShardingNode = RouterUtil.isNoShardingDDL(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            RouterUtil.routeToSingleDDLNode(schemaInfo, rrs, noShardingNode);
            return schemaInfo.getSchemaConfig();
        }
        Map<String, TableConfig> tables = schemaInfo.getSchemaConfig().getTables();
        TableConfig tc = tables.get(schemaInfo.getTable());
        if (tc == null) {
            sc.write(sc.writeToBuffer(OkPacket.OK, sc.allocate()));
            rrs.setFinishedExecute(true);
        } else {
            RouterUtil.routeToDDLNode(schemaInfo, rrs);
        }
        return schemaInfo.getSchemaConfig();
    }
}
