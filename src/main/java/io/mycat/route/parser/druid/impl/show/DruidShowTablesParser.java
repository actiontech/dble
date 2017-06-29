package io.mycat.route.parser.druid.impl.show;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.response.ShowTables;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by huqing.yan on 2017/6/29.
 */
public class DruidShowTablesParser  extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor, ServerConnection sc)
			throws SQLException {
		SQLShowTablesStatement showTablesStatement = (SQLShowTablesStatement) stmt;
		String fromSchema;
		if(showTablesStatement.getDatabase() == null){
			fromSchema = schema.getName();
		}else{
			//MUST BE NOT NULL
			fromSchema = showTablesStatement.getDatabase().getSimpleName();
			if (MycatServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
				fromSchema = fromSchema.toLowerCase();
			}
			Pattern pattern = ShowTables.pattern;
			Matcher ma = pattern.matcher(rrs.getStatement());
			StringBuilder sql = new StringBuilder();
			//MUST RETURN TRUE
			ma.matches();
			sql.append(ma.group(1));
			if(ma.group(2)!=null) {
				sql.append(ma.group(2));
			}
			sql.append(ma.group(3));
			if (ma.group(7) != null) {
				sql.append(ma.group(7));
			}
			rrs.setStatement(sql.toString());
		}
		SchemaConfig schemaToShow = MycatServer.getInstance().getConfig().getSchemas().get(fromSchema);
		RouterUtil.routeToSingleNode(rrs, schemaToShow.getDataNode());
		return schema;
	}
}
