/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.sql.visitor;

import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlEvalVisitorImpl;

public class ActionMySqlEvalVisitorImpl extends MySqlEvalVisitorImpl {
    public boolean visit(SQLBinaryExpr x) {
        return ActionSQLEvalVisitorUtils.visit(this, x);
    }
}
