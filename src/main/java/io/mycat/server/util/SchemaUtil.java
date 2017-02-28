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
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.server.parser.ServerParse;
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

	public static String detectDefaultDb(int type) {
		String db = null;
		if ((ServerParse.SHOW == type || ServerParse.USE == type || ServerParse.EXPLAIN == type
				|| ServerParse.SET == type || ServerParse.HELP == type || ServerParse.DESCRIBE == type)) {
			db = getRandomDb();
		}
		return db;
	}

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
			schemaInfo.schema = StringUtil.removeBackquote(propertyExpr.getOwner().toString());
			schemaInfo.table = StringUtil.removeBackquote(propertyExpr.getName());
		} else if (expr instanceof SQLIdentifierExpr) {
			SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
			schemaInfo.schema = schema;
			schemaInfo.table = StringUtil.removeBackquote(identifierExpr.getName());
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

	public static SchemaInfo isNoSharding(String schema, SQLJoinTableSource tables, SQLStatement stmt)
			throws SQLNonTransientException {
		SchemaInfo returnInfo = null;
		SQLTableSource left = tables.getLeft();
		if (left instanceof SQLExprTableSource) {
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, (SQLExprTableSource) left);
			if (schemaInfo == null) {
				String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
				throw new SQLNonTransientException(msg);
			}
			if (schemaInfo.schemaConfig.isNoSharding()) {
				returnInfo = schemaInfo;
			} else {
				return null;
			}
		} else {
			returnInfo = isNoSharding(schema, (SQLJoinTableSource) left, stmt);
		}
		if (returnInfo == null) {
			return null;
		}
		SQLTableSource right = tables.getLeft();
		if (right instanceof SQLExprTableSource) {
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, (SQLExprTableSource) right);
			if (schemaInfo == null) {
				String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
				throw new SQLNonTransientException(msg);
			}
			if (schemaInfo.schemaConfig.isNoSharding()) {
				returnInfo = schemaInfo;
			} else {
				return null;
			}
		} else {
			returnInfo = isNoSharding(schema, (SQLJoinTableSource) right, stmt);
		}
		return returnInfo;
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
