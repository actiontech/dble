package io.mycat.route.parser.druid.impl.ddl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

import java.sql.SQLException;

public class DruidTruncateTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLException {
		String schemaName = schema == null ? null : schema.getName();
		SQLTruncateStatement truncateTable = (SQLTruncateStatement) stmt;
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(rrs.getSession().getSource().getUser(), schemaName, truncateTable.getTableSources().get(0));
		if (schemaInfo == null) {
			String msg = "No database selected";
			throw new SQLException(msg,"3D000", ErrorCode.ER_NO_DB_ERROR);
		}
		RouterUtil.routeToDDLNode(schemaInfo, rrs);
		return schemaInfo.schemaConfig;
	}
}
