/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.handler;

import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.services.manager.dump.DumpException;
import com.oceanbase.obsharding_d.services.manager.dump.DumpFileContext;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

public class TableHandler extends DefaultHandler {

    @Override
    public SQLStatement preHandle(DumpFileContext context, String stmt) throws SQLSyntaxErrorException {
        stmt = stmt.replace("\r", "");
        SQLStatement sqlStatement = DruidUtil.parseMultiSQL(stmt);
        String tableName;
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            tableName = StringUtil.removeBackQuote(((MySqlCreateTableStatement) sqlStatement).getTableSource().getName().getSimpleName());
            context.setTable(tableName);
            if (!(context.getTableConfig() instanceof ShardingTableConfig)) {
                return null;
            }
            boolean isChanged = preHandleCreateTable(context, sqlStatement);
            return isChanged ? sqlStatement : null;
        } else if (sqlStatement instanceof SQLDropTableStatement) {
            tableName = StringUtil.removeBackQuote(((SQLDropTableStatement) sqlStatement).getTableSources().get(0).getName().getSimpleName());
            context.setTable(tableName);
        } else if (sqlStatement instanceof MySqlLockTableStatement) {
            tableName = StringUtil.removeBackQuote(((MySqlLockTableStatement) sqlStatement).getItems().get(0).getTableSource().getName().getSimpleName());
            context.setTable(tableName);
        }
        return null;
    }

    /**
     * pre handle create table statement
     *
     * @param context
     * @param sqlStatement
     * @return whether statement is changed
     */
    private boolean preHandleCreateTable(DumpFileContext context, SQLStatement sqlStatement) {
        BaseTableConfig tableConfig = context.getTableConfig();
        List<SQLTableElement> columns = ((MySqlCreateTableStatement) sqlStatement).getTableElementList();
        boolean isChanged = false;
        if (tableConfig instanceof ShardingTableConfig) {
            ShardingTableConfig shardingTableConfig = (ShardingTableConfig) tableConfig;
            isChanged = checkColumns(context, columns, shardingTableConfig.getShardingColumn(), shardingTableConfig.getIncrementColumn());
            if (context.getPartitionColumnIndex() == -1) {
                throw new DumpException("can't find partition column in create.");
            }
            // increment column check
            if (shardingTableConfig.getIncrementColumn() != null && context.getIncrementColumnIndex() == -1) {
                throw new DumpException("can't find increment column in create.");
            }
        }
        return isChanged;
    }

    /**
     * if there are create statement in dump file, we check increment column and sharding column index for insert statement.
     *
     * @param context
     * @param columns
     * @return whether column is changed
     */
    private boolean checkColumns(DumpFileContext context, List<SQLTableElement> columns, String shardingColumn, String incrementColumn) {
        SQLTableElement column;
        boolean isChanged = false;
        for (int j = 0; j < columns.size(); j++) {
            column = columns.get(j);
            if (!(columns.get(j) instanceof SQLColumnDefinition)) {
                continue;
            }
            String columnName = StringUtil.removeBackQuote(((SQLColumnDefinition) column).getNameAsString());
            // find index of increment column
            if (incrementColumn != null && columnName.equalsIgnoreCase(incrementColumn)) {
                // check data type of increment column
                // if the column is increment column, data type must be bigint.
                SQLColumnDefinition columnDef = (SQLColumnDefinition) column;
                if (!columnDef.getDataType().getName().equals("bigint")) {
                    context.addError("data type of increment column isn't bigint, OBsharding-D replaced it by itself.");
                    columnDef.setDataType(new SQLCharacterDataType("bigint"));
                    isChanged = true;
                }
                context.setIncrementColumnIndex(j);
            }
            // find index of partition column
            if (shardingColumn != null && columnName.equalsIgnoreCase(shardingColumn)) {
                context.setPartitionColumnIndex(j);
            }
        }
        return isChanged;
    }
}
