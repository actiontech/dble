package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.Date;

public class GlobalTableInsertHandler extends InsertHandler {

    private long time = new Date().getTime();

    @Override
    public void preProcess(DumpFileContext context) throws InterruptedException {
        if (insertHeader == null) {
            return;
        }
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, insertHeader.toString(), true, false);
        }
    }

    @Override
    public void process(DumpFileContext context, SQLInsertStatement.ValuesClause valueClause, boolean isFirst) throws InterruptedException, SQLNonTransientException {
        valueClause.addValue(new SQLIntegerExpr(time));
        super.process(context, valueClause, isFirst);
    }

    @Override
    public void postProcess(DumpFileContext context) throws InterruptedException {
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, ";", false, false);
        }
        super.postProcess(context);
    }
}
