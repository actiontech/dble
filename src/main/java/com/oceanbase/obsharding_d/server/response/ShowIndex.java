/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowIndexesStatement;

import java.util.regex.Pattern;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public final class ShowIndex {
    private ShowIndex() {
    }

    private static final String INDEX_PAT = "^\\s*(show)" +
            "(\\s+(index|indexes|keys))" +
            "(\\s+(from|in)\\s+(((`((?!`).)+`|[a-zA-Z_0-9]+)\\.)?(`((?!`).)+`|[a-zA-Z_0-9]+)))" +
            "(\\s+(from|in)\\s+((`((?!`).)+`|[a-zA-Z_0-9]+)))?" +
            "(\\s+(where)\\s+((. *)*)\\s*)?" +
            "(\\s*\\/\\*[\\s\\S]*\\*\\/)?" +
            "\\s*$";
    public static final Pattern PATTERN = Pattern.compile(INDEX_PAT, Pattern.CASE_INSENSITIVE);

    public static void response(ShardingService shardingService, String stmt) {
        try {
            String table;
            String schema;
            String strWhere = "";
            //show index with where :druid has a bug :no where
            int whereIndex = stmt.toLowerCase().indexOf("where");
            if (whereIndex > 0) {
                strWhere = stmt.substring(whereIndex);
                stmt = stmt.substring(0, whereIndex);
            }
            StringBuilder sql = new StringBuilder();
            boolean changeSQL = false;
            SQLStatement statement = DruidUtil.parseMultiSQL(stmt);
            if (statement instanceof SQLShowIndexesStatement) {
                SQLShowIndexesStatement sqlShowIndexesStatement = (SQLShowIndexesStatement) statement;
                table = StringUtil.removeBackQuote(sqlShowIndexesStatement.getTable().getName().getSimpleName());
                SQLName database = sqlShowIndexesStatement.getDatabase();
                schema = database == null ? shardingService.getSchema() : StringUtil.removeBackQuote(database.getSimpleName());
                if (schema == null) {
                    shardingService.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
                    return;
                }
                if (sqlShowIndexesStatement.getDatabase() != null) {
                    sqlShowIndexesStatement.setDatabase(null);
                    sql.append(sqlShowIndexesStatement.toString());
                    changeSQL = true;
                }

            } else {
                shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, stmt);
                return;
            }
            //show index with where :druid has a bug :no where
            if (changeSQL && whereIndex > 0 && !sql.toString().toLowerCase().contains("where")) {
                sql.append(" ");
                sql.append(strWhere);
            }
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                schema = schema.toLowerCase();
                table = table.toLowerCase();
            }
            SchemaInfo schemaInfo = new SchemaInfo(schema, table);
            shardingService.routeSystemInfoAndExecuteSQL(sql.length() > 0 ? sql.toString() : stmt, schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
