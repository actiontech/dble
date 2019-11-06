package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.Date;
import java.util.List;

public class InsertHandler implements StatementHandler {

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        TableConfig tableConfig = context.getTableConfig();
        MySqlInsertStatement insert = (MySqlInsertStatement) sqlStatement;
        // check columns from insert
        checkColumns(context, insert.getColumns());

        StringBuilder sb = new StringBuilder("INSERT INTO " + context.getTable());
        if (!CollectionUtil.isEmpty(insert.getColumns())) {
            sb.append(insert.getColumns().toString());
        }
        boolean isAutoIncrement = tableConfig.isAutoIncrement();
        long time = new Date().getTime();
        for (SQLInsertStatement.ValuesClause valueClause : insert.getValuesList()) {
            boolean isChanged = false;
            if (isAutoIncrement) {
                if (!handleIncrementColumn(context, valueClause.getValues())) continue;
                isChanged = true;
            }
            if (tableConfig.isGlobalTable() && context.isGlobalCheck()) {
                valueClause.addValue(new SQLIntegerExpr(time));
                isChanged = true;
            }
            if (context.getPartitionColumnIndex() != -1) {
                Integer nodeIndex = handleShardingColumn(context, valueClause.getValues());
                if (nodeIndex == null) {
                    continue;
                }
                String dataNode = tableConfig.getDataNodes().get(nodeIndex);
                context.getWriter().write(dataNode, sb.toString() + valueClause, true);
                continue;
            }
            for (String dataNode : tableConfig.getDataNodes()) {
                context.getWriter().write(dataNode, isChanged ? sb.toString() + valueClause : context.getStmt(), isChanged);
            }
        }
    }

    private void checkColumns(DumpFileContext context, List<SQLExpr> columns) throws DumpException {
        if (columns.isEmpty()) {
            return;
        }

        TableConfig tableConfig = context.getTableConfig();
        int partitionColumnIndex = -1;
        int incrementColumnIndex = -1;
        if (tableConfig.isAutoIncrement() || tableConfig.getPartitionColumn() != null) {
            for (int i = 0; i < columns.size(); i++) {
                SQLExpr column = columns.get(i);
                String columnName = StringUtil.removeBackQuote(column.toString());
                if (columnName.equalsIgnoreCase(tableConfig.getTrueIncrementColumn())) {
                    incrementColumnIndex = i;
                }
                if (columnName.equalsIgnoreCase(tableConfig.getPartitionColumn())) {
                    partitionColumnIndex = i;
                }
            }
            if (tableConfig.isAutoIncrement() && incrementColumnIndex == -1) {
                // add column
                columns.add(new SQLCharExpr(tableConfig.getTrueIncrementColumn()));
                incrementColumnIndex = columns.size();
                boolean incrementIsPartition = tableConfig.getTrueIncrementColumn().equalsIgnoreCase(tableConfig.getPartitionColumn());
                if (incrementIsPartition) {
                    partitionColumnIndex = incrementColumnIndex;
                }
            }

            if (tableConfig.getPartitionColumn() != null && partitionColumnIndex == -1) {
                throw new DumpException("can't find partition column in insert.");
            }
        }
        context.setIncrementColumnIndex(incrementColumnIndex);
        context.setPartitionColumnIndex(partitionColumnIndex);
    }

    private boolean handleIncrementColumn(DumpFileContext context, List<SQLExpr> values) throws DumpException {
        String tableKey = StringUtil.getFullName(context.getSchema(), context.getTable());
        long val = 0;
        try {
            val = SequenceManager.getHandler().nextId(tableKey);
        } catch (SQLNonTransientException e) {
            context.addError(e.getMessage());
            return false;
        }
        int incrementIndex = context.getIncrementColumnIndex();
        if (incrementIndex == values.size() || incrementIndex == -1) {
            values.add(new SQLIntegerExpr(val));
        } else {
            SQLExpr value = values.get(incrementIndex);
            if (!StringUtil.isEmpty(SQLUtils.toMySqlString(value)) && !context.isNeedSkipError()) {
                context.addError("can't set value in increment column, dble has replaced it by itself.");
                context.setNeedSkipError(true);
            }
            values.set(incrementIndex, new SQLIntegerExpr(val));
        }
        return true;
    }

    private Integer handleShardingColumn(DumpFileContext context, List<SQLExpr> values) {
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
            context.addError("Not Supported of Sharding Value EXPR :" + values.toString());
            return null;
        }

        Integer nodeIndex = null;
        try {
            nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null || nodeIndex >= context.getTableConfig().getDataNodes().size()) {
                context.addError("can't find any valid datanode shardingValue" + values.toString());
            }
        } catch (Exception e) {
            context.addError("can't calculate valid datanode shardingValue" + values.toString() + ",due to " + e.getMessage());
        }
        return nodeIndex;
    }

}
