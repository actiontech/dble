package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.Date;

public class GlobalTableInsertHandler extends InsertHandler {

    private long time = new Date().getTime();

    @Override
    public void process(DumpFileContext context, SQLInsertStatement.ValuesClause valueClause, boolean isFirst) throws InterruptedException, SQLNonTransientException {
        valueClause.addValue(new SQLIntegerExpr(time));
        super.process(context, valueClause, isFirst);
    }

}
