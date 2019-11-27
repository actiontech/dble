package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.List;

public class InsertHandler extends DefaultHandler {

    protected StringBuilder insertHeader;

    @Override
    public boolean preHandle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException {
        MySqlInsertStatement insert = (MySqlInsertStatement) sqlStatement;
        // check columns from insert columns
        checkColumns(context, insert.getColumns());
        // add
        insertHeader = new StringBuilder("INSERT INTO ");
        insertHeader.append("`");
        insertHeader.append(context.getTable());
        insertHeader.append("`");
        if (!CollectionUtil.isEmpty(insert.getColumns())) {
            insertHeader.append(insert.getColumns().toString());
        }
        insertHeader.append(" VALUES");
        return false;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        MySqlInsertStatement insert = (MySqlInsertStatement) sqlStatement;
        SQLInsertStatement.ValuesClause valueClause;

        preProcess(context);
        for (int i = 0; i < insert.getValuesList().size(); i++) {
            valueClause = insert.getValuesList().get(i);
            try {
                handleIncrementColumn(context, valueClause.getValues());
                process(context, valueClause, i == 0);
            } catch (SQLNonTransientException e) {
                context.addError(e.getMessage());
            }
        }
        postProcess(context);
    }

    private void handleIncrementColumn(DumpFileContext context, List<SQLExpr> values) throws SQLNonTransientException {
        int incrementIndex = context.getIncrementColumnIndex();
        if (incrementIndex == -1) {
            return;
        }

        String tableKey = StringUtil.getFullName(context.getSchema(), context.getTable());
        long val = SequenceManager.getHandler().nextId(tableKey);
        SQLExpr value = values.get(incrementIndex);
        if (!StringUtil.isEmpty(SQLUtils.toMySqlString(value)) && !context.isNeedSkipError()) {
            context.addError("For table using global sequence, dble has set increment column values for you.");
            context.setNeedSkipError(true);
        }
        values.set(incrementIndex, new SQLIntegerExpr(val));
    }

    private void checkColumns(DumpFileContext context, List<SQLExpr> columns) throws DumpException {
        if (columns.isEmpty()) {
            return;
        }

        TableConfig tableConfig = context.getTableConfig();
        int partitionColumnIndex = -1;
        int incrementColumnIndex = -1;
        boolean isAutoIncrement = tableConfig.isAutoIncrement();
        if (isAutoIncrement || tableConfig.getPartitionColumn() != null) {
            for (int i = 0; i < columns.size(); i++) {
                SQLExpr column = columns.get(i);
                String columnName = StringUtil.removeBackQuote(column.toString());
                if (isAutoIncrement && columnName.equalsIgnoreCase(tableConfig.getTrueIncrementColumn())) {
                    incrementColumnIndex = i;
                }
                if (columnName.equalsIgnoreCase(tableConfig.getPartitionColumn())) {
                    partitionColumnIndex = i;
                }
            }
            // partition column check
            if (tableConfig.getPartitionColumn() != null && partitionColumnIndex == -1) {
                throw new DumpException("can't find partition column in insert.");
            }
            // increment column check
            if (tableConfig.isAutoIncrement() && incrementColumnIndex == -1) {
                throw new DumpException("can't find increment column in insert.");
            }
            context.setIncrementColumnIndex(incrementColumnIndex);
            context.setPartitionColumnIndex(partitionColumnIndex);
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

    public void preProcess(DumpFileContext context) throws InterruptedException {
        if (insertHeader == null) {
            return;
        }
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, insertHeader.toString(), true, false);
        }
    }

    public void postProcess(DumpFileContext context) throws InterruptedException {
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, ";", false, false);
        }
        insertHeader = null;
    }

    public void process(DumpFileContext context, SQLInsertStatement.ValuesClause valueClause, boolean isFirst) throws InterruptedException, SQLNonTransientException {
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, toString(valueClause.getValues(), isFirst), false, false);
        }
    }

}
