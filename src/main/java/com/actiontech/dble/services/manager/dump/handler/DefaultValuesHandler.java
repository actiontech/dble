package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.List;

public class DefaultValuesHandler {


    public void preProcess(DumpFileContext context) {
    }

    public void postProcess(DumpFileContext context) throws InterruptedException {
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, ";");
        }
    }

    public void process(DumpFileContext context, List<SQLExpr> values, boolean isFirst, MySqlInsertStatement statement) throws InterruptedException, SQLNonTransientException {
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, statement);
        }
    }

}
