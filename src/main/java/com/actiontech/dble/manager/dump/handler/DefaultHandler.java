package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLNonTransientException;

public class DefaultHandler implements StatementHandler {

    public enum TableType {
        SHARDING, INCREMENT, DEFAULT
    }

    @Override
    public SQLStatement preHandle(DumpFileContext context, String stmt) throws SQLNonTransientException {
        return null;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, SQLUtils.toMySqlString(sqlStatement), true, true);
        }
    }

    @Override
    public void handle(DumpFileContext context, String stmt) throws InterruptedException {
        if (context.getTable() == null) {
            for (String dataNode : context.getAllDataNodes()) {
                context.getWriter().write(dataNode, stmt);
            }
        } else if (context.getTableConfig() == null) {
            context.getWriter().write(context.getDefaultDataNode(), stmt);
        } else {
            for (String dataNode : context.getTableConfig().getDataNodes()) {
                context.getWriter().write(dataNode, stmt, false, true);
            }
        }
    }
}
