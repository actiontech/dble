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
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;

import java.util.regex.Pattern;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public final class ShowColumns {
    private ShowColumns() {
    }

    private static final String COLUMNS_PAT = "^\\s*(show)" +
            "(\\s+full)?" +
            "(\\s+(columns|fields))" +
            "(\\s+(from|in)\\s+([a-zA-Z_0-9.]+))" +
            "(\\s+(from|in)\\s+([a-zA-Z_0-9]+))?" +
            "((\\s+(like)\\s+'((. *)*)'\\s*)|(\\s+(where)\\s+((. *)*)\\s*))?" +
            "\\s*$";
    public static final Pattern PATTERN = Pattern.compile(COLUMNS_PAT, Pattern.CASE_INSENSITIVE);

    public static void response(ServerConnection c, String stmt) {
        try {
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            MySqlShowColumnsStatement showColumnsStatement = (MySqlShowColumnsStatement) statement;
            String table = StringUtil.removeBackQuote(showColumnsStatement.getTable().getSimpleName());
            String schema = showColumnsStatement.getDatabase() == null ? c.getSchema() : showColumnsStatement.getDatabase().getSimpleName();
            if (schema == null) {
                c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
                return;
            }
            String sql = stmt;
            if (showColumnsStatement.getDatabase() != null) {
                showColumnsStatement.setDatabase(null);
                sql = showColumnsStatement.toString();
            }
            if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
                schema = StringUtil.removeBackQuote(schema).toLowerCase();
                table = table.toLowerCase();
            }
            SchemaInfo schemaInfo = new SchemaInfo(schema, table);
            c.routeSystemInfoAndExecuteSQL(sql, schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
