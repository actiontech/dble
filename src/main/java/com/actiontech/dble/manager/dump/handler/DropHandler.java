package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;

public class DropHandler extends DefaultHandler {

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        SQLDropTableStatement drop = (SQLDropTableStatement) sqlStatement;
        String tableName = drop.getTableSources().get(0).getName().getSimpleName();
        context.setTable(tableName);

        boolean isFinished = preHandle(context);
        if (isFinished) {
            return;
        }

        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, context.getStmt());
        }
    }

}
