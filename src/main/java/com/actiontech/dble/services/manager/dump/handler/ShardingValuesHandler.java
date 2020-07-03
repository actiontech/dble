package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;

import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardingValuesHandler extends DefaultValuesHandler {

    private Map<String, LongPtr> shardingNodes = new HashMap<>(64);

    public void reset() {
        shardingNodes.clear();
    }

    @Override
    public void preProcess(DumpFileContext context) {
    }

    @Override
    public void postProcess(DumpFileContext context) {
    }

    @Override
    public void process(DumpFileContext context, List<SQLExpr> values, boolean isFirst) throws SQLNonTransientException, InterruptedException {
        Integer nodeIndex = handleShardingColumn(context, values);
        String shardingNode = context.getTableConfig().getShardingNodes().get(nodeIndex);
        // sharding table
        LongPtr num = shardingNodes.get(shardingNode);
        if (num == null) {
            shardingNodes.put(shardingNode, new LongPtr(1));
            context.getWriter().writeInsertHeader(shardingNode, insertHeader.toString() + toString(values, true));
            return;
        }
        String stmt;
        if (num.get() < context.getConfig().getMaxValues()) {
            num.incre();
            stmt = toString(values, false);
        } else {
            shardingNodes.put(shardingNode, new LongPtr(1));
            context.getWriter().writeInsertValues(shardingNode, ";");
            context.getWriter().writeInsertHeader(shardingNode, insertHeader.toString());
            stmt = toString(values, true);
        }
        context.getWriter().writeInsertValues(shardingNode, stmt);
    }

    private Integer handleShardingColumn(DumpFileContext context, List<SQLExpr> values) throws SQLNonTransientException {
        AbstractPartitionAlgorithm algorithm = ((ShardingTableConfig) context.getTableConfig()).getFunction();
        SQLExpr expr = values.get(context.getPartitionColumnIndex());
        String shardingValue = null;
        if (expr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
            shardingValue = intExpr.getNumber() + "";
        } else if (expr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) expr;
            shardingValue = charExpr.getText();
        } // no need to consider SQLHexExpr

        if (shardingValue == null && !(expr instanceof SQLNullExpr)) {
            throw new SQLNonTransientException("Not Supported of Sharding Value EXPR :" + values.toString());
        }

        Integer nodeIndex;
        try {
            nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null || nodeIndex >= context.getTableConfig().getShardingNodes().size()) {
                throw new SQLNonTransientException("can't find any valid shardingnode shardingValue" + values.toString());
            }
        } catch (Exception e) {
            throw new SQLNonTransientException("can't calculate valid shardingnode shardingValue" + values.toString() + ",due to " + e.getMessage());
        }
        return nodeIndex;
    }
}
