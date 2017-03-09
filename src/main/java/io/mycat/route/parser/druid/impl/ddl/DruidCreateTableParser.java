package io.mycat.route.parser.druid.impl.ddl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.util.StringUtil;


public class DruidCreateTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {
		MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement)stmt;
		if(createStmt.getSelect() != null) {
			String msg = "create table from other table not supported :" + stmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		String schemaName = schema == null ? null : schema.getName();
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, createStmt.getTableSource());
		if (schemaInfo == null) {
			String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		if (GlobalTableUtil.useGlobleTableCheck()
				&& GlobalTableUtil.isGlobalTable(schemaInfo.schemaConfig, schemaInfo.table)) {
			String sql= addColumnIfCreate(rrs.getStatement(), createStmt);
			rrs.setStatement(sql);
			rrs.setSqlStatement(createStmt);
		}
		rrs = RouterUtil.routeToDDLNode(schemaInfo, rrs);
		return schemaInfo.schemaConfig;
	}

	private String addColumnIfCreate(String sql, MySqlCreateTableStatement createStmt) {
		removeGlobalColumnIfExist(createStmt);
		createStmt.getTableElementList().add(GlobalTableUtil.createMycatColumn());
		return createStmt.toString();
	}

	private static void removeGlobalColumnIfExist(SQLCreateTableStatement statement) {
		for (SQLTableElement tableElement : statement.getTableElementList()) {
			SQLName sqlName = null;
			if (tableElement instanceof SQLColumnDefinition) {
				sqlName = ((SQLColumnDefinition) tableElement).getName();
			}
			if (sqlName != null) {
				String simpleName = sqlName.getSimpleName();
				simpleName = StringUtil.removeBackQuote(simpleName);
				if (tableElement instanceof SQLColumnDefinition && GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN.equalsIgnoreCase(simpleName)) {
					((SQLCreateTableStatement) statement).getTableElementList().remove(tableElement);
					break;
				}
			}
		}
	}
}
