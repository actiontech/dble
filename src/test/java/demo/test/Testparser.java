package demo.test;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

public class Testparser {
	public static void main(String args[]) {
		Testparser obj = new Testparser();
//		obj.test("CREATE TABLE `char_columns_test` (`id` int(11) NOT NULL,`c_char` char(255) DEFAULT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
//		obj.test("CREATE TABLE `xx`.`char_columns_test` (`id` int(11) NOT NULL,`c_char` char(255) DEFAULT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
//		obj.test("drop table char_columns_test;");
//		obj.test("truncate table char_columns_test;");
		
//		obj.test("create index idx_test on char_columns_test(id) ;");
//		obj.test("drop index  idx_test on char_columns_test;");
		String strAlterSql ="";
//		strAlterSql ="alter table char_columns_test add column id2 int(11) NOT NULL after x;";
//		obj.test(strAlterSql);
//		strAlterSql ="alter table char_columns_test add column id2 int(11) NOT NULL,id3 int(11) NOT NULL;";
//		obj.test(strAlterSql);
//		strAlterSql="alter table char_columns_test add index idx_test(id) ;";
//		obj.test(strAlterSql);
//		strAlterSql="alter table char_columns_test add key idx_test(id) ;";
//		obj.test(strAlterSql);
//		strAlterSql="alter table char_columns_test unique key idx_test(id) ;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 ADD CONSTRAINT MyPrimaryKey PRIMARY KEY (id);";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 ADD CONSTRAINT MyUniqueConstraint UNIQUE(c_char);";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 drop index idex_test;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 drop key idex_test;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 CHANGE COLUMN c_char c_varchar varchar(255) DEFAULT NULL  ;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 CHANGE COLUMN c_char c_varchar varchar(255) DEFAULT NULL FIRST;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 CHANGE COLUMN c_char c_varchar varchar(255) DEFAULT NULL AFTER id ;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 modify COLUMN c_char  varchar(255) DEFAULT NULL  ;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 modify COLUMN c_char  varchar(255) DEFAULT NULL FIRST;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 modify COLUMN c_char  varchar(255) DEFAULT NULL AFTER id ;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 DROP COLUMN c_char;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 DROP PRIMARY KEY;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE char_columns1 RENAME  TO  char_columns1_1;";
//		obj.test(strAlterSql);
//		strAlterSql = "ALTER TABLE test_yhq.char_columns1 RENAME  AS  test_yhq2.char_columns1_1;";
//		obj.test(strAlterSql);
		
		String strDeleteSql ="";
//		strDeleteSql = "delete a from  test_yhq.char_columns1 a where 1=1;";
//		obj.test(strDeleteSql);
//		strDeleteSql = "delete from  test_yhq.char_columns1 where 1=1;";
//		obj.test(strDeleteSql);
//		strDeleteSql = "delete a from  test_yhq.char_columns1 a inner join  test_yhq.char_columns1 b on a.id =b.id   where 1=1;";
//		obj.test(strDeleteSql);
//		strDeleteSql = "delete a,b from  test_yhq.char_columns1 a inner join  test_yhq.char_columns1 b on a.id =b.id   where 1=1;";
//		obj.test(strDeleteSql);
//		strDeleteSql = "DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;";
//		obj.test(strDeleteSql);
//		strDeleteSql = "DELETE a, b FROM t1 a INNER JOIN t2 b INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;";
//		obj.test(strDeleteSql);
//		strDeleteSql = "DELETE t1, t2 FROM t1 ,t2 using(id)  where 1=1;";
//		obj.test(strDeleteSql);
		
		
		
//	    String strUpdateSql = "";
//		strUpdateSql = "UPDATE t1 SET col1 = col1 + 1,col2 = col1 ;";
//		obj.test(strUpdateSql);
//		strUpdateSql = "UPDATE t SET id = id + 1 ORDER BY id DESC;";
//		obj.test(strUpdateSql);
//		strUpdateSql = "UPDATE items,month SET items.price=month.price WHERE items.id=month.id;";
//		obj.test(strUpdateSql);
//		strUpdateSql = "UPDATE items inner join month on items.id=month.id SET items.price=month.price;";
//		obj.test(strUpdateSql); 
//		strUpdateSql = "UPDATE   EmployeeS AS P SET      P.LEAGUENO = '2000' WHERE    P.EmployeeNO = 95;";
//		obj.test(strUpdateSql);
		String strCreateIndex = "";
		strCreateIndex = "CREATE INDEX part_of_name ON customer (name(10));";
		obj.test(strCreateIndex);
		strCreateIndex = "CREATE UNIQUE INDEX part_of_name ON customer (name(10));";
		obj.test(strCreateIndex);
	}
	public void test(String sql){
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLStatement statement = parser.parseStatement();
		if(statement instanceof MySqlCreateTableStatement){
			MySqlCreateTableStatement createStment = (MySqlCreateTableStatement)statement;
			SQLExpr expr = createStment.getTableSource().getExpr();
			if (expr instanceof SQLPropertyExpr) {
				SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
				System.out.println((propertyExpr.getOwner().toString()));
				System.out.println(propertyExpr.getName());
			} else if (expr instanceof SQLIdentifierExpr) {
				SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
				System.out.println(identifierExpr.getName());
			} else {
				System.out.println(expr.getClass() + "\n");
			}
			
		}
		else if (statement instanceof SQLAlterTableStatement) {
			SQLAlterTableStatement alterStatement = (SQLAlterTableStatement) statement;
			SQLExprTableSource tableSource = alterStatement.getTableSource();
			for (SQLAlterTableItem alterItem : alterStatement.getItems()) {
				if (alterItem instanceof SQLAlterTableAddColumn) {
					SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) alterItem;
					boolean isFirst = addColumn.isFirst();
					SQLName afterColumn = addColumn.getAfterColumn();
					if (afterColumn != null) {
						System.out.println(sql + ": afterColumns:\n" + afterColumn.getClass().toString() + "\n");
					}
					for (SQLColumnDefinition columnDef : addColumn.getColumns()) {

					}
				} else if (alterItem instanceof SQLAlterTableAddIndex) {
					SQLAlterTableAddIndex addIndex = (SQLAlterTableAddIndex) alterItem;
					SQLName name = addIndex.getName();
					System.out.println(sql + ":indexname:\n" + name.getClass().toString() + "\n");
					String type = addIndex.getType();
					addIndex.isUnique();// ??
					for(SQLSelectOrderByItem item: addIndex.getItems()){
						System.out.println(sql + ": item.getExpr():\n" + item.getExpr().getClass().toString() + "\n");
					}
				} else if (alterItem instanceof SQLAlterTableAddConstraint) {
					SQLAlterTableAddConstraint addConstraint = (SQLAlterTableAddConstraint) alterItem;
					SQLConstraint constraint = addConstraint.getConstraint();
					if(constraint instanceof MySqlUnique){
						MySqlUnique unique = (MySqlUnique)constraint;
						unique.getName();
					}else if(constraint instanceof MySqlPrimaryKey){
						
					}else if(constraint instanceof MysqlForeignKey){
						System.out.println("NOT SUPPORT\n");
					}
					
					System.out.println(sql + ": constraint:\n" + constraint.getClass().toString() + "\n");
				} else if (alterItem instanceof SQLAlterTableDropIndex) {
					SQLAlterTableDropIndex dropIndex = (SQLAlterTableDropIndex) alterItem;
					SQLIdentifierExpr indexName = (SQLIdentifierExpr)dropIndex.getIndexName();
					String strIndexName = indexName.getName();
				} else if (alterItem instanceof SQLAlterTableDropKey) {
					SQLAlterTableDropKey dropIndex = (SQLAlterTableDropKey) alterItem;
					SQLIdentifierExpr indexName = (SQLIdentifierExpr)dropIndex.getKeyName();
					String strIndexName = indexName.getName();
				} else if (alterItem instanceof MySqlAlterTableChangeColumn) {
					MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) alterItem;
					boolean isFirst = changeColumn.isFirst();
					SQLIdentifierExpr afterColumn = (SQLIdentifierExpr)changeColumn.getAfterColumn();
//					SQLExpr afterColumn = changeColumn.getAfterColumn();
					if (afterColumn != null) { 
						String strAfterColumn = afterColumn.getName();
					}
					SQLColumnDefinition columnDef = changeColumn.getNewColumnDefinition();
				} else if (alterItem instanceof MySqlAlterTableModifyColumn) {
					MySqlAlterTableModifyColumn modifyColumn = (MySqlAlterTableModifyColumn) alterItem;
					boolean isFirst = modifyColumn.isFirst();
					SQLExpr afterColumn = modifyColumn.getAfterColumn();
					if (afterColumn != null) {
						System.out.println(sql + ": afterColumns:\n" + afterColumn.getClass().toString() + "\n");
					}
					SQLColumnDefinition columnDef = modifyColumn.getNewColumnDefinition();
				} else if (alterItem instanceof SQLAlterTableDropColumnItem) {
					SQLAlterTableDropColumnItem dropColumn = (SQLAlterTableDropColumnItem) alterItem;
					for (SQLName dropName : dropColumn.getColumns()) {
						System.out.println(sql + ":dropName:\n" + dropName.getClass().toString() + "\n");
					}
				} else if (alterItem instanceof SQLAlterTableDropPrimaryKey) {
					SQLAlterTableDropPrimaryKey dropPrimary = (SQLAlterTableDropPrimaryKey) alterItem;
				} else {
					System.out.println(sql + ":\n" + alterItem.getClass().toString() + "\n");
				}
				System.out.println("\n"+statement.toString());
			}
		} else if (statement instanceof SQLDropTableStatement) { 
		}  else if (statement instanceof SQLTruncateStatement) {
			//TODO:Sequence?
		} else if(statement instanceof SQLDropIndexStatement){
			//TODO
		}else if(statement instanceof MySqlDeleteStatement){
			MySqlDeleteStatement deleteStatement = (MySqlDeleteStatement)statement;
			SQLTableSource tableSource = deleteStatement.getTableSource();
			
			System.out.println(sql + ":getTableSource:" + tableSource.getClass().toString() + "\n");
			if(deleteStatement.getFrom()!= null){
				System.out.println(sql + ":getFrom:" + deleteStatement.getFrom().getClass().toString() + "\n");
			}
			System.out.println("\n");
		} else if(statement instanceof MySqlUpdateStatement){
			MySqlUpdateStatement updateStatement = (MySqlUpdateStatement)statement;
			SQLTableSource tableSource = updateStatement.getTableSource();
			
			System.out.println(sql + ":getTableSource:" + tableSource.getClass().toString() + "\n");
			System.out.println("\n"+statement.toString());
			
		} else if(statement instanceof SQLCreateIndexStatement){
			SQLCreateIndexStatement stament = (SQLCreateIndexStatement)statement;
			SQLTableSource tableSource = stament.getTable();
			System.out.println(sql + ":getTableSource:" + tableSource.getClass().toString() + "\n");
			System.out.println(sql + stament.getType());
		}else{
			// to do further
		}
	}
}
