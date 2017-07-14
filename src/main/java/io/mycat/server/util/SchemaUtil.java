package io.mycat.server.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatPrivileges;
import io.mycat.config.MycatPrivileges.Checktype;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.plan.common.ptr.StringPtr;
import io.mycat.route.parser.druid.impl.SubQueryTableVisitor;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * Created by magicdoom on 2016/1/26.
 */
public class SchemaUtil
{
	public static final String MYSQL_SCHEMA = "mysql";
	public static final String INFORMATION_SCHEMA = "information_schema";
	public static final String TABLE_PROC = "proc";
	public static final String TABLE_PROFILING = "PROFILING";


	public static String getRandomDb() {
		Map<String, SchemaConfig> schemaConfigMap = MycatServer.getInstance().getConfig().getSchemas();
		if (!schemaConfigMap.isEmpty()) {
			return schemaConfigMap.entrySet().iterator().next().getKey();
		}
		return null;
	}

	public static SchemaInfo getSchemaInfo(String user, String schema, SQLExpr expr) throws SQLException {
		SchemaInfo schemaInfo = new SchemaInfo();
		if (expr instanceof SQLPropertyExpr) {
			SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
			schemaInfo.schema = StringUtil.removeBackQuote(propertyExpr.getOwner().toString());
			schemaInfo.table = StringUtil.removeBackQuote(propertyExpr.getName());
		} else if (expr instanceof SQLIdentifierExpr) {
			SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
			schemaInfo.schema = schema;
			schemaInfo.table = StringUtil.removeBackQuote(identifierExpr.getName());
		}
		if (schemaInfo.schema == null) {
			String msg = "No database selected";
			throw new SQLException(msg,"3D000",ErrorCode.ER_NO_DB_ERROR);
		}
		if (MycatServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
			schemaInfo.table = schemaInfo.table.toLowerCase();
			schemaInfo.schema = schemaInfo.schema.toLowerCase();
		}
		if(MYSQL_SCHEMA.equalsIgnoreCase(schemaInfo.schema)||INFORMATION_SCHEMA.equalsIgnoreCase(schemaInfo.schema)){
			return schemaInfo;
		}
		else{
			SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schemaInfo.schema);
			if (schemaConfig == null) {
				String msg = "Table " + StringUtil.getFullName(schemaInfo.schema, schemaInfo.table) + " doesn't exist";
				throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
			}
			if (user != null) {
				UserConfig userConfig = MycatServer.getInstance().getConfig().getUsers().get(user);
				if (!userConfig.getSchemas().contains(schemaInfo.schema)) {
					String msg = " Access denied for user '" + user + "' to database '" + schemaInfo.schema + "'";
					throw new SQLException(msg, "HY000", ErrorCode.ER_DBACCESS_DENIED_ERROR);
				}
			}
			schemaInfo.schemaConfig = schemaConfig;
			return schemaInfo;
		}
	}

	public static SchemaInfo getSchemaInfo(String user, String schema, SQLExprTableSource tableSource) throws SQLException {
		return getSchemaInfo(user, schema, tableSource.getExpr());
	}

	public static boolean isNoSharding(ServerConnection source, SQLSelectQuery sqlSelectQuery, SQLStatement selectStmt, String contextSchema, StringPtr sqlSchema)
			throws SQLException {
		if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mySqlSelectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
			if (!isNoSharding(source, mySqlSelectQueryBlock.getFrom(), selectStmt, contextSchema, sqlSchema)) {
				return false;
			}
			if (mySqlSelectQueryBlock.getWhere() != null && !SchemaUtil.isNoSharding(source, mySqlSelectQueryBlock.getWhere(), contextSchema, sqlSchema)) {
				return false;
			}
			for (SQLSelectItem selectItem : mySqlSelectQueryBlock.getSelectList()) {
				if (!SchemaUtil.isNoSharding(source, selectItem.getExpr(), contextSchema, sqlSchema)) {
					return false;
				}
			}
			return true;
		} else if (sqlSelectQuery instanceof MySqlUnionQuery) {
			return isNoSharding(source, (MySqlUnionQuery) sqlSelectQuery, selectStmt, contextSchema, sqlSchema);
		} else {
			return false;
		}
	}

	public static boolean isNoSharding(ServerConnection source, SQLExpr sqlExpr, String contextSchema, StringPtr sqlSchema)
			throws SQLException {
		SubQueryTableVisitor subQueryTableVisitor = new SubQueryTableVisitor();
		sqlExpr.accept(subQueryTableVisitor);
		SQLSelect sqlSelect = subQueryTableVisitor.getSQLSelect();
		if (sqlSelect != null && !isNoSharding(source, sqlSelect.getQuery(), new SQLSelectStatement(sqlSelect), contextSchema, sqlSchema)) {
			return false;
		}
		return true;
	}

	private static boolean isNoSharding(ServerConnection source, MySqlUnionQuery sqlSelectQuery, SQLStatement stmt, String contextSchema, StringPtr sqlSchema)
			throws SQLException {
		SQLSelectQuery left = sqlSelectQuery.getLeft();
		SQLSelectQuery right = sqlSelectQuery.getRight();
		return isNoSharding(source, left, stmt, contextSchema, sqlSchema) && isNoSharding(source, right, stmt, contextSchema, sqlSchema);
	}
	private static boolean isNoSharding(ServerConnection source, SQLTableSource tables, SQLStatement stmt, String contextSchema, StringPtr sqlSchema)
			throws SQLException {
		if (tables == null) {
			return true;
		} else if (tables instanceof SQLExprTableSource) {
			return isNoSharding(source, (SQLExprTableSource) tables, stmt, contextSchema, sqlSchema);
		} else if (tables instanceof SQLJoinTableSource) {
			return isNoSharding(source, (SQLJoinTableSource) tables, stmt, contextSchema, sqlSchema);
		} else if (tables instanceof SQLSubqueryTableSource) {
			SQLSelectQuery sqlSelectQuery = ((SQLSubqueryTableSource) tables).getSelect().getQuery();
			return isNoSharding(source, sqlSelectQuery, stmt, contextSchema, sqlSchema);
		} else {
			return false;
		}
	}

	private static boolean isNoSharding(ServerConnection source, SQLExprTableSource table, SQLStatement stmt, String contextSchema, StringPtr sqlSchema)
			throws SQLException {
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(source.getUser(), contextSchema, table);
		Checktype chekctype = Checktype.SELECT;
		if (stmt instanceof MySqlUpdateStatement) {
			chekctype = Checktype.UPDATE;
		} else if (stmt instanceof SQLSelectStatement) {
			chekctype = Checktype.SELECT;
		} else if (stmt instanceof MySqlDeleteStatement) {
			chekctype = Checktype.DELETE;
		}

		if (!MycatPrivileges.checkPrivilege(source, schemaInfo.schema, schemaInfo.table, chekctype)) {
			String msg = "The statement DML privilege check is not passed, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		if (!RouterUtil.isNoSharding(schemaInfo.schemaConfig, schemaInfo.table)) {
			return false;
		} else if (sqlSchema.get() == null) {
			sqlSchema.set(schemaInfo.schema);
			return true;
		} else {
			return sqlSchema.get().equals(schemaInfo.schema);
		}
	}
	
	public static boolean isNoSharding(ServerConnection source,  SQLJoinTableSource tables, SQLStatement stmt, String contextSchema, StringPtr sqlSchema)
			throws SQLException {
		SQLTableSource left = tables.getLeft();
		SQLTableSource right = tables.getRight();
		return isNoSharding(source, left, stmt, contextSchema, sqlSchema) && isNoSharding(source, right, stmt, contextSchema, sqlSchema);
	}

    public static class SchemaInfo
    {
        public    String table;
        public    String schema;
        public    SchemaConfig schemaConfig;

        @Override
        public String toString()
        {
			return "SchemaInfo{" + "table='" + table + '\'' +
					", schema='" + schema + '\'' +
					'}';
        }
    }
}
