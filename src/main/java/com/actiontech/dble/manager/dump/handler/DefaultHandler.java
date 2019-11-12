package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

public class DefaultHandler implements StatementHandler {

    @Override
    public boolean preHandle(DumpFileContext context, SQLStatement statement) throws InterruptedException {
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
        String stmt = sqlStatement == null ? context.getStmt() : SQLUtils.toMySqlString(sqlStatement);
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, stmt, sqlStatement != null, true);
        }
    }
}
