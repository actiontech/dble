package io.mycat.route.parser.druid.impl.ddl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

import java.sql.SQLException;

public class DruidTruncateTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        String schemaName = schema == null ? null : schema.getName();
        SQLTruncateStatement truncateTable = (SQLTruncateStatement) stmt;
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, truncateTable.getTableSources().get(0));
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.schema);
        rrs.setStatement(statement);
        if (RouterUtil.isNoSharding(schemaInfo.schemaConfig, schemaInfo.table)) {
            RouterUtil.routeToSingleDDLNode(schemaInfo, rrs);
            return schemaInfo.schemaConfig;
        }
        RouterUtil.routeToDDLNode(schemaInfo, rrs);
        return schemaInfo.schemaConfig;
    }
}
