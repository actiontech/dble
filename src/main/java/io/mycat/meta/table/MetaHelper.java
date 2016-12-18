package io.mycat.meta.table;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import io.mycat.meta.protocol.MyCatMeta.ColumnMeta;
import io.mycat.meta.protocol.MyCatMeta.IndexMeta;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;

public class MetaHelper {
	public static enum INDEX_TYPE{
		PRI,UNI,MUL
	}
	public static String PRIMARY ="PRIMARY";
	public static TableMeta initTableMeta(String table, SQLCreateTableStatement createStment, long timeStamp) {
		TableMeta.Builder tmBuilder = TableMeta.newBuilder();
		tmBuilder.setTableName(table);
		tmBuilder.setVersion(timeStamp);
		for (SQLTableElement tableElement : createStment.getTableElementList()) {
			if (tableElement instanceof SQLColumnDefinition) {
				ColumnMeta.Builder cmBuilder = makeColumnMeta((SQLColumnDefinition) tableElement);
				if(cmBuilder.getAutoIncre()){
					tmBuilder.setAiColPos(tmBuilder.getColumnsCount());
				}
				tmBuilder.addColumns(cmBuilder.build());
			} else if (tableElement instanceof MySqlPrimaryKey) {
				MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) tableElement;
				tmBuilder.setPrimary(makeIndexMeta(PRIMARY,  INDEX_TYPE.PRI, primaryKey.getColumns()));
			} else if (tableElement instanceof MySqlUnique) {
				MySqlUnique unique = (MySqlUnique) tableElement;
				tmBuilder.addUniIndex(makeIndexMeta(unique.getIndexName().getSimpleName(), INDEX_TYPE.UNI, unique.getColumns()));
			} else if (tableElement instanceof MySqlTableIndex) {
				MySqlTableIndex index = (MySqlTableIndex) tableElement;
				tmBuilder.addIndex(makeIndexMeta(index.getName().getSimpleName(), INDEX_TYPE.MUL, index.getColumns()));
			} else {
				// ignore
			}
		}
		return tmBuilder.build();
	}

	public static IndexMeta makeIndexMeta(String indexName, INDEX_TYPE indexType, List<SQLExpr> columnExprs) {
		IndexMeta.Builder indexBuilder = IndexMeta.newBuilder();
		indexBuilder.setName(indexName);
		indexBuilder.setType(indexType.toString());
		for (int i = 0; i < columnExprs.size(); i++) {
			SQLIdentifierExpr column = (SQLIdentifierExpr) columnExprs.get(i);
			indexBuilder.addColumns(column.getName()); 
		}
		return indexBuilder.build();
	}

	public static ColumnMeta.Builder makeColumnMeta(SQLColumnDefinition column) {
		ColumnMeta.Builder cmBuilder = ColumnMeta.newBuilder();
		cmBuilder.setName(column.getName().getSimpleName());
		cmBuilder.setDataType(column.getDataType().getName());
		for (SQLColumnConstraint constraint : column.getConstraints()) {
			if (constraint instanceof SQLNotNullConstraint) {
				cmBuilder.setCanNull(false);
			} else if (constraint instanceof SQLNullConstraint) {
				cmBuilder.setCanNull(true);
			} else {
				// SQLColumnPrimaryKey ,SQLColumnUniqueKey will not happen in "show create table ..", ignore
			}
		}
		if (column.getDefaultExpr() != null) {
			StringBuilder builder = new StringBuilder();
			MySqlOutputVisitor visitor = new MySqlOutputVisitor(builder);
			column.getDefaultExpr().accept(visitor);
			cmBuilder.setSdefault(builder.toString());
		}
		if (column.isAutoIncrement()) {
			cmBuilder.setAutoIncre(true);
		}
		return cmBuilder;
	}
}
