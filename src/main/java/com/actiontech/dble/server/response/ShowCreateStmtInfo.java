/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huqing.yan on 2017/7/20.
 */
public class ShowCreateStmtInfo {
    private static final String TABLE_PAT = "^\\s*(show)" +
            "(\\s+full)?" +
            "(\\s+tables)" +
            "(\\s+(from|in)\\s+([a-zA-Z_0-9]+))?" +
            "((\\s+(like)\\s+'((. *)*)'\\s*)|(\\s+(where)\\s+((. *)*)\\s*))?" +
            "\\s*$";
    public static final Pattern PATTERN = Pattern.compile(TABLE_PAT, Pattern.CASE_INSENSITIVE);
    private final boolean isFull;
    private final String schema;
    private final String cond;
    private final String like;
    private final String where;
    private final SQLExpr whereExpr;

    public ShowCreateStmtInfo(String sql) throws SQLSyntaxErrorException {
        Matcher ma = PATTERN.matcher(sql);
        ma.matches(); //always RETURN TRUE
        isFull = ma.group(2) != null;
        schema = ma.group(6);
        cond = ma.group(7);
        like = ma.group(10);
        where = ma.group(14);
        SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(sql);
        whereExpr = ((SQLShowTablesStatement) statement).getWhere();
    }

    public boolean isFull() {
        return isFull;
    }

    public String getSchema() {
        return schema;
    }

    public String getCond() {
        return cond;
    }

    public String getLike() {
        return like;
    }

    public String getWhere() {
        return where;
    }

    public SQLExpr getWhereExpr() {
        return whereExpr;
    }
}
