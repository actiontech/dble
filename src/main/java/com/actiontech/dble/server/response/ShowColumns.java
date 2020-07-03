/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.factory.RouteStrategyFactory;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public final class ShowColumns {
    private ShowColumns() {
    }

    private static final String COLUMNS_PAT = "^\\s*(show)(\\s+full)?(\\s+(columns|fields))" +
            "(\\s+(from|in)\\s+(((`((?!`).)+`|[a-zA-Z_0-9]+)\\.)?(`((?!`).)+`|[a-zA-Z_0-9]+)))" +
            "(\\s+(from|in)\\s+((`((?!`).)+`|[a-zA-Z_0-9]+)))?" +
            "((\\s+(like)\\s+'((. *)*)'\\s*)" +
            "|(\\s+(where)\\s+((. *)*)\\s*))?\\s*$";
    public static final Pattern PATTERN = Pattern.compile(COLUMNS_PAT, Pattern.CASE_INSENSITIVE);

    public static void response(ShardingService shardingService, String stmt) {
        try {
            Pattern pattern = ShowColumns.PATTERN;
            Matcher ma = pattern.matcher(stmt);
            // always match
            if (ma.matches()) {
                int start = ma.start(6);
                int end = ma.end(6);
                String sub = stmt.substring(0, start);
                String sub2 = stmt.substring(end, stmt.length());
                stmt = sub + " from " + sub2;
            }

            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            MySqlShowColumnsStatement showColumnsStatement = (MySqlShowColumnsStatement) statement;
            String table = StringUtil.removeBackQuote(showColumnsStatement.getTable().getSimpleName());
            SQLName database = showColumnsStatement.getDatabase();
            String schema = database == null ? shardingService.getSchema() : StringUtil.removeBackQuote(database.getSimpleName());
            if (schema == null) {
                shardingService.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
                return;
            }
            String sql = stmt;
            if (showColumnsStatement.getDatabase() != null) {
                showColumnsStatement.setDatabase(null);
                sql = showColumnsStatement.toString();
            }
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                schema = schema.toLowerCase();
                table = table.toLowerCase();
            }
            SchemaInfo schemaInfo = new SchemaInfo(schema, table);
            shardingService.routeSystemInfoAndExecuteSQL(sql, schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
