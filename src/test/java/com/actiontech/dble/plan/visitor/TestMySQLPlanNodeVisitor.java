/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.visitor;

import com.actiontech.dble.plan.node.PlanNode;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * this test must start server, because TableNode need the metadata of tables.
 * Curent, we skip it.
 */
public class TestMySQLPlanNodeVisitor {
    @Ignore
    @Test
    public void testNoraml() {
        PlanNode tableNode = getPlanNode("select * from tvistor");
        System.out.println(tableNode);
        Assert.assertEquals(true, tableNode != null);
    }

    private PlanNode getPlanNode(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement ast = (SQLSelectStatement) parser.parseStatement();
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor("TESTDB", 33, null, true, null);
        visitor.visit(ast);
        return visitor.getTableNode();
    }
}
