package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardingTableInsertHandler extends InsertHandler {

    private Map<String, LongPtr> dataNodes = new HashMap<>(64);

    @Override
    public void preProcess(DumpFileContext context) {
        if (!dataNodes.isEmpty()) {
            dataNodes.clear();
        }
    }

    @Override
    public void postProcess(DumpFileContext context) throws InterruptedException {
        for (String dataNode : dataNodes.keySet()) {
            context.getWriter().write(dataNode, ";", false, false);
        }
        insertHeader = null;
    }

    @Override
    public void process(DumpFileContext context, SQLInsertStatement.ValuesClause valueClause, boolean isFirst) throws SQLNonTransientException, InterruptedException {
        Integer nodeIndex = handleShardingColumn(context, valueClause.getValues());
        String dataNode = context.getTableConfig().getDataNodes().get(nodeIndex);
        // sharding table
        LongPtr num = dataNodes.get(dataNode);
        if (num == null) {
            dataNodes.put(dataNode, new LongPtr(1));
            context.getWriter().write(dataNode, insertHeader.toString(), true, false);
            context.getWriter().write(dataNode, toString(valueClause.getValues(), true), false, false);
            return;
        }
        String stmt;
        if (num.get() <= context.getConfig().getMaxValues()) {
            num.incre();
            stmt = toString(valueClause.getValues(), false);
        } else {
            dataNodes.put(dataNode, new LongPtr(1));
            context.getWriter().write(dataNode, ";", false, false);
            context.getWriter().write(dataNode, insertHeader.toString(), true, false);
            stmt = toString(valueClause.getValues(), true);
        }
        context.getWriter().write(dataNode, stmt, false, false);
    }

    private Integer handleShardingColumn(DumpFileContext context, List<SQLExpr> values) throws SQLNonTransientException {
        AbstractPartitionAlgorithm algorithm = context.getTableConfig().getRule().getRuleAlgorithm();
        SQLExpr expr = values.get(context.getPartitionColumnIndex());
        String shardingValue = null;
        if (expr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
            shardingValue = intExpr.getNumber() + "";
        } else if (expr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) expr;
            shardingValue = charExpr.getText();
        }

        if (shardingValue == null && !(expr instanceof SQLNullExpr)) {
            throw new SQLNonTransientException("Not Supported of Sharding Value EXPR :" + values.toString());
        }

        Integer nodeIndex;
        try {
            nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null || nodeIndex >= context.getTableConfig().getDataNodes().size()) {
                throw new SQLNonTransientException("can't find any valid datanode shardingValue" + values.toString());
            }
        } catch (Exception e) {
            throw new SQLNonTransientException("can't calculate valid datanode shardingValue" + values.toString() + ",due to " + e.getMessage());
        }
        return nodeIndex;
    }
}
