package io.mycat.server.response;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import io.mycat.config.ErrorCode;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.util.SchemaUtil;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public class ShowCreateTable {
	public static void response(ServerConnection c, String stmt){
		try {
			SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
			MySqlShowCreateTableStatement showCreateTableStatement = (MySqlShowCreateTableStatement) statement;
			SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(c.getUser(), c.getSchema(), showCreateTableStatement.getName());
			c.routeSystemInfoAndExecuteSQL(RouterUtil.removeSchema(stmt, schemaInfo.schema), schemaInfo, ServerParse.SHOW);
		} catch (Exception e) {
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
		}
	}
}
