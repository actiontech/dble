package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import java.util.List;

public class CreateTableHandler extends DefaultHandler {

    @Override
    public boolean preHandle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        MySqlCreateTableStatement create = (MySqlCreateTableStatement) sqlStatement;
        String tableName = StringUtil.removeBackQuote(create.getTableSource().getName().getSimpleName());
        context.setTable(tableName);
        if (super.preHandle(context, sqlStatement)) {
            return true;
        }

        TableConfig tableConfig = context.getTableConfig();
        List<SQLTableElement> columns = create.getTableElementList();
        if (tableConfig.isAutoIncrement() || tableConfig.getPartitionColumn() != null) {
            // check columns for sharing column index or increment column index
            checkColumns(context, columns);
            // partition column check
            if (tableConfig.getPartitionColumn() != null && context.getPartitionColumnIndex() == -1) {
                throw new DumpException("can't find partition column in create.");
            }
            // increment column check
            if (tableConfig.isAutoIncrement() && context.getIncrementColumnIndex() == -1) {
                throw new DumpException("can't find increment column in create.");
            }
        }
        return false;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        boolean isChanged = false;
        List<SQLTableElement> columns = ((MySqlCreateTableStatement) sqlStatement).getTableElementList();
        TableConfig tableConfig = context.getTableConfig();
        if (context.getIncrementColumnIndex() != -1) {
            // check data type of increment column
            SQLColumnDefinition column = (SQLColumnDefinition) columns.get(context.getIncrementColumnIndex());
            if (!column.getDataType().getName().equals("bigint")) {
                context.addError("data type of increment column isn't bigint, dble replaced it by itself.");
                column.setDataType(new SQLCharacterDataType("bigint"));
                isChanged = true;
            }
        }

        // if table is global, add column
        if (tableConfig.isGlobalTable() && context.isGlobalCheck()) {
            columns.add(GlobalTableUtil.createCheckColumn());
            isChanged = true;
        }

        String stmt = isChanged ? SQLUtils.toMySqlString(sqlStatement) : context.getStmt();
        for (String dataNode : tableConfig.getDataNodes()) {
            context.getWriter().write(dataNode, stmt);
        }
    }

    private void checkColumns(DumpFileContext context, List<SQLTableElement> columns) {
        SQLTableElement column;
        TableConfig tableConfig = context.getTableConfig();
        boolean isAutoIncrement = tableConfig.isAutoIncrement();
        for (int j = 0; j < columns.size(); j++) {
            column = columns.get(j);
            if (!(columns.get(j) instanceof SQLColumnDefinition)) {
                continue;
            }
            String columnName = StringUtil.removeBackQuote(((SQLColumnDefinition) column).getNameAsString());
            if (isAutoIncrement && columnName.equalsIgnoreCase(tableConfig.getTrueIncrementColumn())) {
                context.setIncrementColumnIndex(j);
            }
            if (columnName.equalsIgnoreCase(tableConfig.getPartitionColumn())) {
                context.setPartitionColumnIndex(j);
            }
        }
    }
}
