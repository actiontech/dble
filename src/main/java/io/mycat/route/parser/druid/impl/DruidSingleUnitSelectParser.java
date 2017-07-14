package io.mycat.route.parser.druid.impl;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;

import io.mycat.MycatServer;
import io.mycat.config.MycatPrivileges;
import io.mycat.config.MycatPrivileges.Checktype;
import io.mycat.config.model.SchemaConfig;
import io.mycat.plan.common.ptr.StringPtr;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.handler.MysqlInformationSchemaHandler;
import io.mycat.server.handler.MysqlProcHandler;
import io.mycat.server.response.InformationSchemaProfiling;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;

public class DruidSingleUnitSelectParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
									 MycatSchemaStatVisitor visitor, ServerConnection sc) throws SQLException {
		SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		String schemaName = schema == null ? null : schema.getName();
		if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();
			SQLTableSource mysqlFrom = mysqlSelectQuery.getFrom();
			if (mysqlFrom == null) {
				if(schema == null) {
					String db = SchemaUtil.getRandomDb();
					if (db == null) {
						String msg = "No schema is configured, make sure your config is right, sql:" + stmt;
						throw new SQLNonTransientException(msg);
					}
					schema = MycatServer.getInstance().getConfig().getSchemas().get(db);
				}
				rrs = RouterUtil.routeToSingleNode(rrs, schema.getMetaDataNode());
				rrs.setFinishedRoute(true);
				return schema;
			}
			if (mysqlFrom instanceof SQLSubqueryTableSource || mysqlFrom instanceof SQLJoinTableSource || mysqlFrom instanceof SQLUnionQueryTableSource) {
				StringPtr sqlSchema = new StringPtr(null);
				if (SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt,schemaName, sqlSchema)) {
					String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
					SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(realSchema);
					rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
					RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
					return schemaConfig;
				} else{
					super.visitorParse(schema, rrs, stmt, visitor, sc);
					return schema;
				}
			}
			
			SQLExprTableSource fromSource = (SQLExprTableSource) mysqlFrom;
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, fromSource);
			// 兼容PhpAdmin's, 支持对MySQL元数据的模拟返回
			if (SchemaUtil.INFORMATION_SCHEMA.equals(schemaInfo.schema)) {
				MysqlInformationSchemaHandler.handle(schemaInfo, sc);
				rrs.setFinishedExecute(true);
				return schema;
			}

			if (SchemaUtil.MYSQL_SCHEMA.equals(schemaInfo.schema)
					&& SchemaUtil.TABLE_PROC.equals(schemaInfo.table)) {
				// 兼容MySQLWorkbench
				MysqlProcHandler.handle(rrs.getStatement(), sc);
				rrs.setFinishedExecute(true);
				return schema;
			}
			// fix navicat SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS
			// `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS
			// `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID=
			// GROUP BY STATE ORDER BY SEQ
			if (SchemaUtil.INFORMATION_SCHEMA.equals(schemaInfo.schema)
					&& SchemaUtil.TABLE_PROFILING.equals(schemaInfo.table)
					&& rrs.getStatement().toUpperCase().contains("CONCAT(ROUND(SUM(DURATION)/*100,3)")) {
				InformationSchemaProfiling.response(sc);
				rrs.setFinishedExecute(true);
				return schema;
			}
			if (schemaInfo.schemaConfig == null) {
				String msg = "No Supported, sql:" + stmt;
				throw new SQLNonTransientException(msg);
			}
			if (!MycatPrivileges.checkPrivilege(sc, schemaInfo.schema, schemaInfo.table, Checktype.SELECT)) {
				String msg = "The statement DML privilege check is not passed, sql:" + stmt;
				throw new SQLNonTransientException(msg);
			}
			rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.schema));
			schema = schemaInfo.schemaConfig;
			super.visitorParse(schema, rrs, stmt, visitor, sc);
			// 更改canRunInReadDB属性
			if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode())
					&& !sc.isAutocommit()) {
				rrs.setCanRunInReadDB(false);
			}
		} else if (sqlSelectQuery instanceof MySqlUnionQuery) {
			StringPtr sqlSchema = new StringPtr(null);
			if (SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt,schemaName, sqlSchema)) {
				String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
				SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(realSchema);
				rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
				RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
				return schemaConfig;
			} else{
				super.visitorParse(schema, rrs, stmt, visitor, sc);
			}
		}
		return schema;
	} 
}
