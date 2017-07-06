package io.mycat.route.parser.druid.impl.ddl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidDropTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor, ServerConnection sc)
			throws SQLException {
		SQLDropTableStatement dropTable = (SQLDropTableStatement) stmt;
		if(dropTable.getTableSources().size()>1){
			String msg = "dropping multi-tables is not supported, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		String schemaName = schema == null ? null : schema.getName();
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, dropTable.getTableSources().get(0));
		String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.schema);
		rrs.setStatement(statement);
		if(RouterUtil.isNoSharding(schemaInfo.schemaConfig,schemaInfo.table)){
			RouterUtil.routeToSingleDDLNode(schemaInfo, rrs);
			return schemaInfo.schemaConfig;
		}
		RouterUtil.routeToDDLNode(schemaInfo, rrs);
		return schemaInfo.schemaConfig;
	}
}
