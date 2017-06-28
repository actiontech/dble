package io.mycat.route.parser.druid.impl.ddl;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.GlobalTableUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.util.StringUtil;


public class DruidCreateTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor, ServerConnection sc)
			throws SQLException {
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
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, createStmt.getTableSource());
		if (schemaInfo == null) {
			String msg = "No database selected";
			throw new SQLException(msg,"3D000", ErrorCode.ER_NO_DB_ERROR);
		}

		//如果这个不是no_sharing表格那么就需要这么进行检查
		if(!RouterUtil.isNoSharding(schemaInfo.schemaConfig,schemaInfo.table)){
			sharingTableCheck(createStmt);
		}

		if (GlobalTableUtil.useGlobleTableCheck()
				&& GlobalTableUtil.isGlobalTable(schemaInfo.schemaConfig, schemaInfo.table)) {
			String sql= addColumnIfCreate(createStmt);
			rrs.setStatement(sql);
			rrs.setSqlStatement(createStmt);
		}
		RouterUtil.routeToDDLNode(schemaInfo, rrs);
		return schemaInfo.schemaConfig;
	}

	private String addColumnIfCreate(MySqlCreateTableStatement createStmt) {
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
				if (GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN.equalsIgnoreCase(simpleName)) {
					statement.getTableElementList().remove(tableElement);
					break;
				}
			}
		}
	}

	/**
	 * 检查创建的表格里面是不是有我们不支持的参数
	 */
	private void sharingTableCheck(MySqlCreateTableStatement createStmt) throws SQLNonTransientException {
		//创建新表分片属性禁止
		if(createStmt.getPartitioning() != null){
			String msg = "create table with Partition not supported:" + createStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		//创建的新表只允许出现InnoDB引擎
		SQLObject engine = createStmt.getTableOptions().get("ENGINE");
		if (engine != null) {
			String strEngine;
			if (engine instanceof SQLCharExpr) {
				strEngine = ((SQLCharExpr) engine).getText();
			} else if (engine instanceof SQLIdentifierExpr) {
				strEngine = ((SQLIdentifierExpr) engine).getSimpleName();
			} else {
				strEngine = engine.toString();
			}
			if (!"InnoDB".equalsIgnoreCase(strEngine)) {
				String msg = "create table only can use ENGINE InnoDB,others not supported:" + createStmt;
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			}
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
