/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

/**
 * @author dcy
 * Create Date: 2020-12-25
 */
public class PrepareChangeVisitor extends MySqlASTVisitorAdapter {
    @Override
    public boolean visit(MySqlSelectQueryBlock x) {

        /*
            Change sql.
            append the  '1!=1' condition.
            So every sql  return field packets with zero rows.
         */
        final SQLExpr sqlExpr = SQLUtils.buildCondition(SQLBinaryOperator.BooleanAnd, SQLUtils.toSQLExpr("1 != 1"), true, x.getWhere());
        x.setWhere(sqlExpr);
        /*
            in single node mysql ,one '1!=1' condition is enough.It always return zero rows.
            because dble split complex query before send.So every query should append this condition.So...there need return true to access nested select.
         */
        return true;
    }


}
