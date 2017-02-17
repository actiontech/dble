package io.mycat.plan.visitor;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.plan.PlanNode;

public class TestMySQLPlanNodeVisitor {
	@Test
	public void testNoraml() {
		 PlanNode tableNode = getPlanNode("select * from table1");
		 System.out.println(tableNode);
		 Assert.assertEquals(true, tableNode !=null  );
	}
	private PlanNode getPlanNode(String sql){
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLSelectStatement ast = (SQLSelectStatement)parser.parseStatement();
		MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor("test");
		visitor.visit(ast);
		return visitor.getTableNode();
	}
}
