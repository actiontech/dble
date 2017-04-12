package io.mycat.server.util;

import java.sql.SQLNonTransientException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import io.mycat.MycatServer;
import io.mycat.config.MycatPrivileges;
import io.mycat.config.MycatPrivileges.Checktype;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

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
        if(ma.matches()&&ma.groupCount()>=5)
        {
          return  ma.group(5);
        }
        return null;
    }

	public static SchemaInfo getSchemaInfo(String schema, SQLExprTableSource tableSource) {
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
			return null;
		}
		if (MycatServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
			schemaInfo.table = schemaInfo.table.toLowerCase();
			schemaInfo.schema = schemaInfo.schema.toLowerCase();
		}
		SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schemaInfo.schema);
		if (schemaConfig == null && !MYSQL_SCHEMA.equalsIgnoreCase(schemaInfo.schema)&& !INFORMATION_SCHEMA.equalsIgnoreCase(schemaInfo.schema)) {
			return null;
		}
		schemaInfo.schemaConfig = schemaConfig;
		return schemaInfo;
	}
	
	public static SchemaInfo isNoSharding(ServerConnection source,String schema, SQLSelectQuery sqlSelectQuery, SQLStatement selectStmt)
			throws SQLNonTransientException {
		if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			return isNoSharding(source, schema, ((MySqlSelectQueryBlock) sqlSelectQuery).getFrom(), selectStmt);
		} else if (sqlSelectQuery instanceof MySqlUnionQuery) {
			return isNoSharding(source, schema, (MySqlUnionQuery) sqlSelectQuery, selectStmt);
		} else {
			return null;
		}
	}
	private static SchemaInfo isNoSharding(ServerConnection source,String schema, MySqlUnionQuery sqlSelectQuery, SQLStatement stmt)
			throws SQLNonTransientException {
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
			throws SQLNonTransientException {
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
			throws SQLNonTransientException {
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, table);
		if (schemaInfo == null) {
			String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
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
			throws SQLNonTransientException {
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
            final StringBuffer sb = new StringBuffer("SchemaInfo{");
            sb.append("table='").append(table).append('\'');
            sb.append(", schema='").append(schema).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

private  static     Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(FULL)*\\s*(TABLES)\\s+(FROM)\\s+([a-zA-Z_0-9]+)\\s*([a-zA-Z_0-9\\s]*)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args)
    {
        String sql = "SELECT name, type FROM `mysql`.`proc` as xxxx WHERE Db='base'";
     //   System.out.println(parseSchema(sql));
        sql="insert into aaa.test(id) values(1)" ;
       // System.out.println(parseSchema(sql));
        sql="update updatebase.test set xx=1 " ;
        //System.out.println(parseSchema(sql));

        String pat3 = "show  full  tables from  base like ";
        Matcher ma = pattern.matcher(pat3);
        if(ma.matches())
        {
            System.out.println(ma.groupCount());
            System.out.println(ma.group(5));
        }



    }
}
