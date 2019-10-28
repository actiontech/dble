package com.actiontech.dble.manager.handler.dump.type.handler;

import com.actiontech.dble.manager.handler.dump.type.DumpTable;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

public class ShardingTableHandler extends DumpTableHandler {

    @Override
    String postAfterProcessCreate(MySqlCreateTableStatement insert) throws SQLSyntaxErrorException {
        return null;
    }

    @Override
    String postAfterProcessInsert(MySqlInsertStatement insert, DumpTable table) throws SQLSyntaxErrorException {
        boolean isIncrement = table.getTableConfig().isAutoIncrement();
        int incrementColumnIndex = table.getIncrementColumnIndex();
        if (isIncrement && incrementColumnIndex == -1) {
            throw new SQLSyntaxErrorException("can't find increment column[" + table.getTableConfig().getTrueIncrementColumn() + "]");
        }

        int partitionColumnIndex = table.getPartitionColumnIndex();
        if (!table.getTableConfig().isNoSharding() && partitionColumnIndex == -1) {
            throw new SQLSyntaxErrorException("can't find partition column" + table.getTableConfig().getPartitionColumn() + "]");
        }

        AbstractPartitionAlgorithm algorithm = table.getTableConfig().getRule().getRuleAlgorithm();
        Map<String, StringBuilder> inserts = new HashMap<>(5);
        for (SQLInsertStatement.ValuesClause valueClause : insert.getValuesList()) {
            // 全局序列
            if (isIncrement) {
                SQLExpr value = valueClause.getValues().get(incrementColumnIndex);
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

            SQLExpr expr = valueClause.getValues().get(partitionColumnIndex);
            String shardingValue = null;
            if (expr instanceof SQLIntegerExpr) {
                SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
                shardingValue = intExpr.getNumber() + "";
            } else if (expr instanceof SQLCharExpr) {
                SQLCharExpr charExpr = (SQLCharExpr) expr;
                shardingValue = charExpr.getText();
            }

            if (shardingValue == null && !(expr instanceof SQLNullExpr)) {
                throw new SQLSyntaxErrorException("Not Supported of Sharding Value EXPR :" + expr.toString());
            }

            Integer nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null) {
                throw new SQLSyntaxErrorException("can't find any valid datanode shardingValue");
            }
            String dataNode = table.getTableConfig().getDataNodes().get(nodeIndex);
            StringBuilder sb = inserts.get(dataNode);
            if (sb == null) {
                sb = new StringBuilder("INSERT INTO " + table.getTableName() + " VALUES(");
                inserts.put(dataNode, sb);
            }

            for (SQLExpr e : valueClause.getValues()) {
                sb.append(e.toString());
                sb.append(",");
            }
            sb.append("),");
        }
        table.setInserts(inserts);
        return null;
    }
}
