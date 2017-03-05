package io.mycat.route.parser.druid.impl.ddl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

public class DruidCreateIndexParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
			MycatSchemaStatVisitor visitor) throws SQLNonTransientException {
		SQLCreateIndexStatement createStmt = (SQLCreateIndexStatement) stmt;
		SQLTableSource tableSource = createStmt.getTable();
		if (tableSource instanceof SQLExprTableSource) {
			String schemaName = schema == null ? null : schema.getName();
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, (SQLExprTableSource) tableSource);
			if (schemaInfo == null) {
				String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
				throw new SQLNonTransientException(msg);
			}
			rrs = RouterUtil.routeToDDLNode(schemaInfo, rrs);
			return schemaInfo.schemaConfig;
		} else {
			String msg = "The DDL is not supported, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
	}
}
