/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement;

import java.util.regex.Pattern;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public final class ShowIndex {
    private ShowIndex() {
    }

    private static final String INDEX_PAT = "^\\s*(show)" +
            "(\\s+(index|indexes|keys))" +
            "(\\s+(from|in)\\s+([a-zA-Z_0-9.]+))" +
            "(\\s+(from|in)\\s+([a-zA-Z_0-9]+))?" +
            "(\\s+(where)\\s+((. *)*)\\s*)?" +
            "\\s*$";
    public static final Pattern PATTERN = Pattern.compile(INDEX_PAT, Pattern.CASE_INSENSITIVE);

    public static void response(ServerConnection c, String stmt) {
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
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            if (statement instanceof MySqlShowIndexesStatement) {
                MySqlShowIndexesStatement mySqlShowIndexesStatement = (MySqlShowIndexesStatement) statement;
                table = StringUtil.removeBackQuote(mySqlShowIndexesStatement.getTable().getSimpleName());
                schema = mySqlShowIndexesStatement.getDatabase() == null ? c.getSchema() : mySqlShowIndexesStatement.getDatabase().getSimpleName();
                if (schema == null) {
                    c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
                    return;
                }
                if (mySqlShowIndexesStatement.getDatabase() != null) {
                    mySqlShowIndexesStatement.setDatabase(null);
                    sql.append(mySqlShowIndexesStatement.toString());
                    changeSQL = true;
                }

            } else if (statement instanceof MySqlShowKeysStatement) {
                MySqlShowKeysStatement mySqlShowKeysStatement = (MySqlShowKeysStatement) statement;
                table = StringUtil.removeBackQuote(mySqlShowKeysStatement.getTable().getSimpleName());
                schema = mySqlShowKeysStatement.getDatabase() == null ? c.getSchema() : mySqlShowKeysStatement.getDatabase().getSimpleName();
                if (schema == null) {
                    c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
                    return;
                }
                if (mySqlShowKeysStatement.getDatabase() != null) {
                    mySqlShowKeysStatement.setDatabase(null);
                    sql.append(mySqlShowKeysStatement.toString());
                    changeSQL = true;
                }
            } else {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, stmt);
                return;
            }
            //show index with where :druid has a bug :no where
            if (changeSQL && whereIndex > 0 && !sql.toString().toLowerCase().contains("where")) {
                sql.append(" ");
                sql.append(strWhere);
            }
            if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
                schema = StringUtil.removeBackQuote(schema).toLowerCase();
                table = table.toLowerCase();
            }
            SchemaInfo schemaInfo = new SchemaInfo(schema, table);
            c.routeSystemInfoAndExecuteSQL(sql.length() > 0 ? sql.toString() : stmt, schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
