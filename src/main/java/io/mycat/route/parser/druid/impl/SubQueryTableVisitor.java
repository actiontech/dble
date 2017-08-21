package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

/**
 * Created by huqing.yan on 2017/7/13.
 */
public class SubQueryTableVisitor extends MySqlASTVisitorAdapter {
    private SQLSelect sqlSelect = null;

    public SQLSelect getSQLSelect() {
        return sqlSelect;
    }

    @Override
    public void endVisit(SQLInSubQueryExpr x) {
        SQLSelect sqlSelect = x.getSubQuery();
        this.sqlSelect = sqlSelect;
    }

    @Override
    public void endVisit(SQLQueryExpr x) {
        SQLSelect sqlSelect = x.getSubQuery();
        this.sqlSelect = sqlSelect;
    }
}
