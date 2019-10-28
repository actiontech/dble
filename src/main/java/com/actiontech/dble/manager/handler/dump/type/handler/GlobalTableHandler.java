package com.actiontech.dble.manager.handler.dump.type.handler;

import com.actiontech.dble.manager.handler.dump.type.DumpTable;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Date;

public class GlobalTableHandler extends DumpTableHandler {

    @Override
    String postAfterProcessCreate(MySqlCreateTableStatement create) throws SQLSyntaxErrorException {
        // add column
        create.getTableElementList().add(GlobalTableUtil.createCheckColumn());
        //        SQLColumnDefinition column = new SQLColumnDefinition();
        //        column.setDataType(new SQLCharacterDataType("bigint"));
        //        column.setDefaultExpr(new SQLNullExpr());
        //        column.setName(new SQLIdentifierExpr("`" + GLOBAL_TABLE_CHECK_COLUMN + "`"));
        //        column.setComment(new SQLCharExpr("field for checking consistency"));
        //        create.getTableElementList().add(column);
        return SQLUtils.toMySqlString(create);
    }

    @Override
    String postAfterProcessInsert(MySqlInsertStatement insert, DumpTable table) throws SQLSyntaxErrorException {
        boolean isIncrement = table.getTableConfig().isAutoIncrement();
        int incrementColumnIndex = table.getIncrementColumnIndex();
        if (isIncrement && incrementColumnIndex == -1) {
            // error
            throw new SQLSyntaxErrorException("can't find increment column");
        }

        if (!insert.getColumns().isEmpty()) {
            // add column
            insert.getColumns().add(new SQLAllColumnExpr());
        }
        long currentTime = new Date().getTime();
        for (SQLInsertStatement.ValuesClause valueClause : insert.getValuesList()) {
            // 全局序列
            if (isIncrement) {
                SQLExpr value = valueClause.getValues().get(incrementColumnIndex);
                // warn
                if (!StringUtil.isEmpty(SQLUtils.toMySqlString(value))) {
                    throw new SQLSyntaxErrorException("can't set value in increment column");
                }
                String tableKey = StringUtil.getFullName(table.getSchema(), table.getTableName());
                try {
                    valueClause.getValues().set(incrementColumnIndex, new SQLIntegerExpr(SequenceManager.getHandler().nextId(tableKey)));
                } catch (SQLNonTransientException e) {
                    throw new SQLSyntaxErrorException(e.getMessage());
                }
            }
            // 新增全局表检查列数据
            valueClause.addValue(new SQLIntegerExpr(currentTime));
        }
        return SQLUtils.toMySqlString(insert);
    }
}
