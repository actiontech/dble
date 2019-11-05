package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import java.util.List;

public class CreateTableHandler extends DefaultHandler {

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        MySqlCreateTableStatement create = (MySqlCreateTableStatement) sqlStatement;
        String tableName = StringUtil.removeBackQuote(create.getTableSource().getName().getSimpleName());
        context.setTable(tableName);

        boolean isFinished = preHandle(context);
        if (isFinished) {
            return;
        }

        boolean isChanged = false;
        TableConfig tableConfig = context.getTableConfig();
        // check column
        List<SQLTableElement> columns = create.getTableElementList();
        if (tableConfig.isAutoIncrement() || tableConfig.getPartitionColumn() != null) {
            checkColumns(context, columns);
            // add increment column if not exists
            if (tableConfig.isAutoIncrement() && context.getIncrementColumnIndex() == -1) {
                SQLColumnDefinition column = new SQLColumnDefinition();
                column.setDataType(new SQLCharacterDataType("bigint"));
                column.setDefaultExpr(new SQLNullExpr());
                column.setName(new SQLIdentifierExpr(tableConfig.getTrueIncrementColumn()));
                columns.add(column);
                isChanged = true;
            }

            // partition column check
            if (tableConfig.getPartitionColumn() != null && context.getPartitionColumnIndex() == -1) {
                throw new DumpException("table[" + context.getTable() + "] can't find partition column in create.");
            }
        }

        if (tableConfig.isGlobalTable() && context.isGlobalCheck()) {
            // if table is global, add column
            columns.add(GlobalTableUtil.createCheckColumn());
            isChanged = true;
        }

        String stmt = isChanged ? SQLUtils.toMySqlString(create) : context.getStmt();
        for (String dataNode : tableConfig.getDataNodes()) {
            context.getWriter().write(dataNode, stmt, isChanged);
        }
    }

    private void checkColumns(DumpFileContext context, List<SQLTableElement> columns) {
        TableConfig tableConfig = context.getTableConfig();
        for (int j = 0; j < columns.size(); j++) {
            SQLTableElement column = columns.get(j);
            if (!(columns.get(j) instanceof SQLColumnDefinition)) {
                continue;
            }
            String columnName = StringUtil.removeBackQuote(((SQLColumnDefinition) column).getNameAsString());
            if (columnName.equalsIgnoreCase(tableConfig.getTrueIncrementColumn())) {
                context.setIncrementColumnIndex(j);
            }
            if (columnName.equalsIgnoreCase(tableConfig.getPartitionColumn())) {
                context.setPartitionColumnIndex(j);
            }
        }
    }
}
