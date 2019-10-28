package com.actiontech.dble.manager.handler.dump.type.handler;

import com.actiontech.dble.manager.handler.dump.type.DumpContent;
import com.actiontech.dble.manager.handler.dump.type.DumpTable;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

public abstract class DumpTableHandler implements DumpHandler {

    @Override
    public void handle(DumpContent content) throws SQLSyntaxErrorException {
        DumpTable table = (DumpTable) content;
        String stmt;
        while (table.hasNext()) {
            stmt = table.get();
            // process create table
            if (stmt.contains("create table") || stmt.contains("CREATE TABLE")) {
                String tempStmt = processCreate(stmt, table);
                table.replace("\n" + tempStmt + "\n", false);
            }

            // process insert table
            if (stmt.contains("insert into") || stmt.contains("INSERT INTO")) {
                String tempStmt = processInsert(stmt, table);
                if (tempStmt != null) {
                    table.replace(tempStmt, true);
                }
            }
        }
    }

    private String processCreate(String stmt, DumpTable table) throws SQLSyntaxErrorException {
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
        List<SQLTableElement> columns = createStmt.getTableElementList();

        String incrementColumn = table.getTableConfig().getTrueIncrementColumn();
        String partitionColumn = table.getTableConfig().getPartitionColumn();
        for (int j = 0; j < columns.size(); j++) {
            SQLTableElement column = columns.get(j);
            if (!(columns.get(j) instanceof SQLColumnDefinition)) {
                continue;
            }
            String columnName = StringUtil.removeBackQuote(((SQLColumnDefinition) column).getNameAsString());
            if (columnName.equalsIgnoreCase(incrementColumn)) {
                table.setIncrementColumnIndex(j);
            }
            if (columnName.equalsIgnoreCase(partitionColumn)) {
                table.setPartitionColumnIndex(j);
            }
        }
        String newStmt = postAfterProcessCreate(createStmt);
        if (newStmt == null) {
            return stmt;
        }
        return newStmt;
    }

    private String processInsert(String stmt, DumpTable table) throws SQLSyntaxErrorException {
        MySqlInsertStatement insert = (MySqlInsertStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
        String newStmt = postAfterProcessInsert(insert, table);
        if (newStmt == null) {
            return stmt;
        }
        return newStmt;
    }

    abstract String postAfterProcessCreate(MySqlCreateTableStatement insert) throws SQLSyntaxErrorException;

    abstract String postAfterProcessInsert(MySqlInsertStatement insert, DumpTable table) throws SQLSyntaxErrorException;

}
