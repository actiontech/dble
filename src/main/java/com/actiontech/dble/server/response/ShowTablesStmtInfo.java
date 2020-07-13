/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huqing.yan on 2017/7/20.
 */
public class ShowTablesStmtInfo {
    private static final String TABLE_PAT = "^\\s*(/\\*[\\s\\S]*\\*/)?\\s*(show)" +
            "(\\s+(full|all))?" +
            "(\\s+tables)" +
            "(\\s+(from|in)\\s+(`((?!`).)+`|[a-zA-Z_0-9]+))?" +
            "((\\s+(like)\\s+'((. *)*)'\\s*)|(\\s+(where)\\s+((. *)*)\\s*))?" +
            "\\s*\\s*(/\\*[\\s\\S]*\\*/)?\\s*$";
    public static final Pattern PATTERN = Pattern.compile(TABLE_PAT, Pattern.CASE_INSENSITIVE);
    private final boolean isFull;
    private final boolean isAll;
    private final String schema;
    private final String cond;
    private final SQLExpr whereExpr;
    private String like;
    private String where;

    public ShowTablesStmtInfo(String sql) throws SQLSyntaxErrorException {
        Matcher ma = PATTERN.matcher(sql);
        ma.matches(); //always RETURN TRUE
        isFull = ma.group(3) != null;
        isAll = isFull && ma.group(4).equalsIgnoreCase("all");
        schema = ma.group(8) == null ? null : StringUtil.removeBackQuote(ma.group(8));
        cond = ma.group(10);

        StringBuilder sb = new StringBuilder(ma.group(2));
        if (isFull) {
            sb.append(" full ");
        }
        sb.append(ma.group(5));
        if (ma.group(6) != null) {
            if (ma.group(7).equalsIgnoreCase("in")) {
                sb.append(" from ");
                sb.append(schema);
            } else {
                sb.append(ma.group(6));
            }
        }
        if (cond != null)
            sb.append(cond);
        sql = sb.toString();
        SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(sql);
        whereExpr = ((SQLShowTablesStatement) statement).getWhere();
        if (whereExpr != null) {
            where = whereExpr.toString();
        }
        SQLExpr likeExpr = ((SQLShowTablesStatement) statement).getLike();
        if (likeExpr != null) {
            like = StringUtil.removeApostrophe(likeExpr.toString());
        }
    }

    public boolean isAll() {
        return isAll;
    }

    public boolean isFull() {
        return isFull;
    }

    public String getSchema() {
        return schema;
    }

    String getCond() {
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
