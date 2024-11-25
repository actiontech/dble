/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.List;

/**
 * @author dcy
 * Create Date: 2020-12-25
 */
public class PrepareChangeVisitor extends MySqlASTVisitorAdapter {
    boolean isFirstSelectQuery = true;

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {

        /*
            Change sql.
            append the  '1!=1' condition.
            So every sql  return field packets with zero rows.
         */

        if (x.getFrom() != null) {
            final SQLExpr sqlExpr = SQLUtils.buildCondition(SQLBinaryOperator.BooleanAnd, SQLUtils.toSQLExpr("1!=1"), true, x.getWhere());
            x.setWhere(sqlExpr);
        } else {
            //if a sql without reference any table. it shouldn't add '1!=1' condition.  Although it works in msyql 8.0 but cause syntax error in mysql 5.7.
        }

        x.setGroupBy(null);
        x.setOrderBy(null);
        /*
            Double guarantee
        */
        x.setLimit(new SQLLimit(new SQLIntegerExpr(0)));
        if (isFirstSelectQuery) {
            final List<String> commentsDirect = x.getBeforeCommentsDirect();
            final String comment = "/* used for prepare statement. */";
            if (commentsDirect != null) {
                commentsDirect.add(0, comment);
            } else {
                x.addBeforeComment(comment);
            }

            isFirstSelectQuery = false;
        }

        /*
            in single node mysql ,one '1!=1' condition is enough.It always return zero rows.
            because OBsharding-D split complex query before send.So every query should append this condition.So...there need return true to access nested select.
         */
        return true;
    }


    @Override
    public boolean visit(SQLVariantRefExpr x) {
        if (x.getParent() != null && x.getParent() instanceof SQLSelectItem) {
            ((SQLSelectItem) x.getParent()).replace(x, SQLUtils.toSQLExpr("true"));
            return false;
        }
        SQLObject parent;
        SQLObject current = x.getParent();

        //nest lookup. find closest SQLReplaceable and replace if possible.
        while (current != null && (parent = current.getParent()) != null) {

            if (parent instanceof SQLReplaceable) {
                if (current instanceof SQLExpr) {
                    ((SQLReplaceable) parent).replace((SQLExpr) current, SQLUtils.toSQLExpr("true"));
                    break;
                } else {
                    current = parent;
                }
            } else {
                current = parent;

            }
        }
        return false;
    }


}
