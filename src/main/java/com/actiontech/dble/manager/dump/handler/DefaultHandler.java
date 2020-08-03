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
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, SQLUtils.toMySqlString(sqlStatement), true, true);
        }
    }

    @Override
    public void handle(DumpFileContext context, String stmt) throws InterruptedException {
        if (context.getTable() == null) {
            for (String shardingNode : context.getAllShardingNodes()) {
                context.getWriter().write(shardingNode, stmt);
            }
        } else if (context.getTableConfig() == null) {
            context.getWriter().write(context.getDefaultShardingNode(), stmt);
        } else {
            for (String shardingNode : context.getTableConfig().getShardingNodes()) {
                context.getWriter().write(shardingNode, stmt, false, true);
            }
        }
    }
}
