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
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.mycat.server.parser.ServerParseShow.TABLE_PAT;

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

    public static String parseShowTableSchema(String sql)
    {
        Matcher ma = pattern.matcher(sql);
        if(ma.matches()&&ma.groupCount()>=6)
        {
          return  ma.group(6);
        }
        return null;
    }
	public static SchemaInfo getSchemaInfo(String user, String schema, SQLExprTableSource tableSource) throws SQLException {
		SchemaInfo schemaInfo = new SchemaInfo();
		SQLExpr expr = tableSource.getExpr();
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
	
	public static SchemaInfo isNoSharding(ServerConnection source,String schema, SQLSelectQuery sqlSelectQuery, SQLStatement selectStmt)
			throws SQLException {
		if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			return isNoSharding(source, schema, ((MySqlSelectQueryBlock) sqlSelectQuery).getFrom(), selectStmt);
		} else if (sqlSelectQuery instanceof MySqlUnionQuery) {
			return isNoSharding(source, schema, (MySqlUnionQuery) sqlSelectQuery, selectStmt);
		} else {
			return null;
		}
	}
	private static SchemaInfo isNoSharding(ServerConnection source,String schema, MySqlUnionQuery sqlSelectQuery, SQLStatement stmt)
			throws SQLException {
		SQLSelectQuery left = sqlSelectQuery.getLeft();
		SQLSelectQuery right = sqlSelectQuery.getRight();
		SchemaInfo leftInfo = isNoSharding(source, schema, left, stmt);
		if (leftInfo == null) {
			return null;
		}
		SchemaInfo rightInfo = isNoSharding(source, schema, right, stmt);
		if (rightInfo == null) {
			return null;
		}
		return StringUtil.equals(leftInfo.schema, rightInfo.schema)?leftInfo:null;
	}
	private static SchemaInfo isNoSharding(ServerConnection source,String schema, SQLTableSource tables, SQLStatement stmt)
			throws SQLException {
		if (tables instanceof SQLExprTableSource) {
			return isNoSharding(source, schema, (SQLExprTableSource) tables, stmt);
		} else if (tables instanceof SQLJoinTableSource) {
			return isNoSharding(source, schema, (SQLJoinTableSource) tables, stmt);
		} else if (tables instanceof SQLSubqueryTableSource) {
			SQLSelectQuery sqlSelectQuery = ((SQLSubqueryTableSource) tables).getSelect().getQuery();
			return isNoSharding(source, schema, sqlSelectQuery, stmt);
		} else {
			return null;
		}
	}

	private static SchemaInfo isNoSharding(ServerConnection source, String schema, SQLExprTableSource table, SQLStatement stmt)
			throws SQLException {
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(source.getUser(),schema, table);
		Checktype chekctype = Checktype.SELECT;
		if (stmt instanceof MySqlUpdateStatement) {
			chekctype = Checktype.UPDATE;
		} else if (stmt instanceof SQLSelectStatement) {
			chekctype = Checktype.SELECT;
		} else if (stmt instanceof MySqlDeleteStatement) {
			chekctype = Checktype.DELETE;
		}

		if(!MycatPrivileges.checkPrivilege(source, schemaInfo.schema, schemaInfo.table, chekctype)){
			String msg = "The statement DML privilege check is not passed, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		if (RouterUtil.isNoSharding(schemaInfo.schemaConfig, schemaInfo.table)) {
			return schemaInfo;
		} else {
			return null;
		}
	}
	
	public static SchemaInfo isNoSharding(ServerConnection source, String schema, SQLJoinTableSource tables, SQLStatement stmt)
			throws SQLException {
		SQLTableSource left = tables.getLeft();
		SQLTableSource right = tables.getRight();
		SchemaInfo leftInfo = isNoSharding(source, schema, left, stmt);
		if (leftInfo == null) {
			return null;
		}
		SchemaInfo rightInfo = isNoSharding(source, schema, right, stmt);
		if (rightInfo == null) {
			return null;
		}
		return StringUtil.equals(leftInfo.schema, rightInfo.schema)?leftInfo:null;
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

private  static     Pattern pattern = Pattern.compile(TABLE_PAT, Pattern.CASE_INSENSITIVE);

}
