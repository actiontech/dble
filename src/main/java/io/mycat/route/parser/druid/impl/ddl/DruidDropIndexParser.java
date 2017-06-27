package io.mycat.route.parser.druid.impl.ddl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

import java.sql.SQLException;

/**
 * 
 * @author huqing.yan
 *
 */
public class DruidDropIndexParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLException {
		String schemaName = schema == null ? null : schema.getName();
		SQLDropIndexStatement dropStmt = (SQLDropIndexStatement) stmt;
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(rrs.getSession().getSource().getUser(), schemaName, dropStmt.getTableName());
		if (schemaInfo == null) {
			String msg = "No database selected";
			throw new SQLException(msg,"3D000", ErrorCode.ER_NO_DB_ERROR);
		}
		RouterUtil.routeToDDLNode(schemaInfo, rrs);
		return schemaInfo.schemaConfig;
	}
}
