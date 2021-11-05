package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.List;

public class ShardingValuesHandler extends DefaultValuesHandler {

    @Override
    public void preProcess(DumpFileContext context) {
    }

    @Override
    public void postProcess(DumpFileContext context) {
    }

    @Override
    public void process(DumpFileContext context, List<SQLExpr> values, boolean isFirst, MySqlInsertStatement table) throws SQLNonTransientException, InterruptedException {
        Integer nodeIndex = handleShardingColumn(context, values);
        String shardingNode = context.getTableConfig().getShardingNodes().get(nodeIndex);
        context.getWriter().writeInsertHeader(shardingNode, table);
    }

    private Integer handleShardingColumn(DumpFileContext context, List<SQLExpr> values) throws SQLNonTransientException {
        AbstractPartitionAlgorithm algorithm = ((ShardingTableConfig) context.getTableConfig()).getFunction();
        SQLExpr expr = values.get(context.getPartitionColumnIndex());
        String shardingValue = null;
        if (expr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
            shardingValue = intExpr.getNumber().toString();
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
