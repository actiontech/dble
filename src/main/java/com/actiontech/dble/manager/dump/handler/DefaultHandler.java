package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLStatement;

public class DefaultHandler implements StatementHandler {

    protected boolean preHandle(DumpFileContext context) throws InterruptedException {
        if (context.getTable() == null) {
            for (String dataNode : DbleServer.getInstance().getConfig().getSchemas().get(context.getSchema()).getAllDataNodes()) {
                context.getWriter().write(dataNode, context.getStmt());
            }
            return true;
        } else if (context.getTableConfig() == null) {
            context.getWriter().write(context.getDefaultDataNode(), context.getStmt());
            return true;
        }
        return false;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        boolean isFinished = preHandle(context);
        if (isFinished) {
            return;
        }

        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, context.getStmt());
        }
    }
}
