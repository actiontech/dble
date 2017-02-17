package io.mycat.route.parser.druid.impl.ddl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

public class DruidDropTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {
		SQLDropTableStatement dropTable = (SQLDropTableStatement) stmt;
		if(dropTable.getTableSources().size()>1){
			String msg = "dropping multi-tables is not supported, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		String schemaName = schema == null ? null : schema.getName();
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, dropTable.getTableSources().get(0));
		if (schemaInfo == null) {
			String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		rrs = RouterUtil.routeToDDLNode(schemaInfo, rrs, ctx.getSql());
		return schemaInfo.schemaConfig;
	}
}
