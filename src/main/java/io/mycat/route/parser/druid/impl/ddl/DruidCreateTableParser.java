package io.mycat.route.parser.druid.impl.ddl;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
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

import java.sql.SQLNonTransientException;


public class DruidCreateTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {
		MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement)stmt;
		//创建新表select from禁止
		if(createStmt.getSelect() != null) {
			String msg = "create table from other table not supported :" + stmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		//创建新表like禁止
		if(createStmt.getLike() != null){
			String msg = "create table like other table not supported :" + stmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		String schemaName = schema == null ? null : schema.getName();
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, createStmt.getTableSource());
		if (schemaInfo == null) {
			String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}

		//如果这个不是no_sharing表格那么就需要这么进行检查
		if(schema.getTables().get(createStmt.getTableSource().toString()).getRule() != null){
			sharingTableCheck(createStmt);
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

	/**
	 * 检查创建的表格里面是不是有我们不支持的参数
	 * @throws SQLNonTransientException
	 */
	private void sharingTableCheck(MySqlCreateTableStatement createStmt) throws SQLNonTransientException {
		//创建新表分片属性禁止
		if(createStmt.getPartitioning() != null){
			String msg = "create table with Partition not supported:" + createStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		//创建的新表只允许出现InnoDB引擎
		SQLIdentifierExpr engineConf = (SQLIdentifierExpr)createStmt.getTableOptions().get("ENGINE");
		if(engineConf != null && !"InnoDB".equalsIgnoreCase(engineConf.getName())){
			String msg = "create table only can use ENGINE InnoDB,others not supported:" + createStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		//创建新表的时候数据目录指定禁止
		if (createStmt.getTableOptions().get("DATA DIRECTORY") != null){
			String msg = "create table with DATA DIRECTORY  not supported:" + createStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		// 创建新表的时候自增属性禁止
		if(createStmt.getTableOptions().get("AUTO_INCREMENT") != null){
			String msg = "create table with AUTO_INCREMENT not supported:" + createStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
	}
}
