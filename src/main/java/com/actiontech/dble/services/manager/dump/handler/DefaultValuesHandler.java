package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLExpr;

import java.sql.SQLNonTransientException;
import java.util.List;

public class DefaultValuesHandler {

    StringBuilder insertHeader;

    void setInsertHeader(StringBuilder insertHeader) {
        this.insertHeader = insertHeader;
    }

    public void preProcess(DumpFileContext context) throws InterruptedException {
        if (insertHeader == null) {
            return;
        }
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, insertHeader.toString(), true, false);
        }
    }

    public void postProcess(DumpFileContext context) throws InterruptedException {
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, ";", false, false);
        }
    }

    public void process(DumpFileContext context, List<SQLExpr> values, boolean isFirst) throws InterruptedException, SQLNonTransientException {
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, toString(values, isFirst), false, false);
        }
    }

    protected String toString(List<SQLExpr> values, boolean isFirst) {
        StringBuilder sbValues = new StringBuilder();
        if (!isFirst) {
            sbValues.append(",");
        }
        sbValues.append("(");
        for (int i = 0; i < values.size(); i++) {
            if (i != 0) {
                sbValues.append(",");
            }
            sbValues.append(values.get(i).toString());
        }
        sbValues.append(")");
        return sbValues.toString();
    }

}
