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
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) {
	}
	
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		String schemaName = schema == null ? null : schema.getName();
		SQLCreateIndexStatement createStmt = (SQLCreateIndexStatement) stmt;
		SQLTableSource tableSource = createStmt.getTable();
		if (tableSource instanceof SQLExprTableSource) {
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, (SQLExprTableSource) tableSource);
			if (schemaInfo == null) {
				String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
				throw new SQLNonTransientException(msg);
			}
			rrs = RouterUtil.routeToDDLNode(schemaInfo, rrs, ctx.getSql());
		} else {
			String msg = "The DDL is not supported, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
	}
}
