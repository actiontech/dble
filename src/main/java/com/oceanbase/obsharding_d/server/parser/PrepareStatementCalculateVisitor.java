/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.parser;

import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

/**
 * @author dcy
 * Create Date: 2020-12-24
 */
public class PrepareStatementCalculateVisitor extends MySqlASTVisitorAdapter {
    private int argumentCount = 0;
    private boolean selectStatement = false;

    public PrepareStatementCalculateVisitor() {
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        argumentCount++;
        return false;
    }


    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        selectStatement = true;
        return true;
    }


    public boolean isSelectStatement() {
        return selectStatement;
    }

    public int getArgumentCount() {
        return argumentCount;
    }

    //    private <T extends SQLObject> void accept(List<T> list) {
    //        if (list != null) {
    //            list.forEach(node -> node.accept(this));
    //        }
    //    }

    //    private <T extends SQLObject> void accept(T e) {
    //        if (e != null) {
    //            e.accept(this);
    //        }
    //    }


}
