package io.mycat.plan.visitor;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.plan.PlanNode;

/**
 * this test must setup mycat server, because TableNode need the metadata of tables.
 * Curent, we skip it.
 */
public class TestMySQLPlanNodeVisitor {
    	@Ignore
	@Test
	public void testNoraml() {
		 PlanNode tableNode = getPlanNode("select * from tvistor");
		 System.out.println(tableNode);
		 Assert.assertEquals(true, tableNode !=null  );
	}
	private PlanNode getPlanNode(String sql){
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLSelectStatement ast = (SQLSelectStatement)parser.parseStatement();
		MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor("TESTDB", 33);
		visitor.visit(ast);
		return visitor.getTableNode();
	}
}
