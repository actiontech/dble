package io.mycat.route.parser.druid.impl.ddl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
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
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.meta.protocol.MyCatMeta.ColumnMeta;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.util.StringUtil;

/**
 * alter table 语句解析
 * @author wang.dw
 *
 */
public class DruidAlterTableParser extends DefaultDruidParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {
		SQLAlterTableStatement alterTable = (SQLAlterTableStatement) stmt;
		String schemaName = schema == null ? null : schema.getName();
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, alterTable.getTableSource());
		if (schemaInfo == null) {
			String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		boolean support = false;
		for (SQLAlterTableItem alterItem : alterTable.getItems()) {
			if (alterItem instanceof SQLAlterTableAddColumn
					|| alterItem instanceof SQLAlterTableAddIndex
					|| alterItem instanceof SQLAlterTableDropIndex
					|| alterItem instanceof SQLAlterTableDropKey
					|| alterItem instanceof MySqlAlterTableChangeColumn
					|| alterItem instanceof MySqlAlterTableModifyColumn
					|| alterItem instanceof SQLAlterTableDropColumnItem
					|| alterItem instanceof SQLAlterTableDropPrimaryKey) {
				support = true;
			} else if (alterItem instanceof SQLAlterTableAddConstraint) {
				SQLConstraint constraint = ((SQLAlterTableAddConstraint) alterItem).getConstraint();
				if (constraint instanceof MySqlPrimaryKey) {
					support = true;
				}
			}
		}
		if (!support) {
			String msg = "THE DDL is not supported, sql:" + stmt;
			throw new SQLNonTransientException(msg);
		}
		if (GlobalTableUtil.useGlobleTableCheck()
				&& GlobalTableUtil.isGlobalTable(schemaInfo.schemaConfig, schemaInfo.table)) {
			String sql = modifyColumnIfAlter(schemaInfo, rrs.getStatement(), alterTable);
			rrs.setStatement(sql);
			rrs.setSqlStatement(alterTable);
		}
		rrs = RouterUtil.routeToDDLNode(schemaInfo, rrs);
		return schemaInfo.schemaConfig;
	}

	

	private String modifyColumnIfAlter(SchemaInfo schemaInfo, String sql, SQLAlterTableStatement alterStatement) throws SQLNonTransientException {
		TableMeta orgTbMeta = MycatServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.schema, schemaInfo.table);
		if (orgTbMeta == null)
			return sql;
		List<String> cols = new ArrayList<String>();
		for (ColumnMeta column : orgTbMeta.getColumnsList()) {
			cols.add(column.getName());
		}
		List<SQLAlterTableItem> newAlterItems = new ArrayList<SQLAlterTableItem>();
		for (SQLAlterTableItem alterItem : alterStatement.getItems()) {
			if (alterItem instanceof SQLAlterTableAddColumn) {
				addColumn(cols, (SQLAlterTableAddColumn) alterItem, newAlterItems);
			} else if (alterItem instanceof MySqlAlterTableChangeColumn) {
				changeColumn(cols, (MySqlAlterTableChangeColumn) alterItem, newAlterItems);
			} else if (alterItem instanceof MySqlAlterTableModifyColumn) {
				modifyColumn(cols, (MySqlAlterTableModifyColumn) alterItem, newAlterItems);
			} else if (alterItem instanceof SQLAlterTableDropColumnItem) {
				dropColumn(cols, (SQLAlterTableDropColumnItem) alterItem, newAlterItems);
			} else {
				newAlterItems.add(alterItem);
			}
		}
		alterStatement.getItems().clear();
		if(newAlterItems.size()==0){
			String msg = "you can't drop the column "+GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN+", sql:" + alterStatement;
			throw new SQLNonTransientException(msg);
		}
		for (SQLAlterTableItem newAlterItem : newAlterItems) {
			alterStatement.addItem(newAlterItem);
		}
		return alterStatement.toString();
	}

	private void dropColumn(List<String> cols, SQLAlterTableDropColumnItem dropColumn, List<SQLAlterTableItem> newAlterItems) {
		for (SQLName dropName : dropColumn.getColumns()) {
			String dropColName = StringUtil.removeBackquote(dropName.getSimpleName());
			if (dropColName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
				continue;
			}
			removeOldCol(cols, dropColName);
			newAlterItems.add(dropColumn);
		}
	}

	private void modifyColumn(List<String> cols, MySqlAlterTableModifyColumn modifyColumn, List<SQLAlterTableItem> newAlterItems) {
		String modifyColName = StringUtil.removeBackquote(modifyColumn.getNewColumnDefinition().getName().getSimpleName());
		MySqlAlterTableModifyColumn newModifyColumn = modifyColumn;
		if (modifyColName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
			removeOldCol(cols, modifyColName);
			newModifyColumn.setNewColumnDefinition(GlobalTableUtil.createMycatColumn());
			newModifyColumn.setAfterColumn(new SQLIdentifierExpr(cols.get(cols.size() - 1)));
			cols.add(modifyColName);
		} else {
			SQLExpr afterColumn = modifyColumn.getAfterColumn();
			if (afterColumn != null) {
				removeOldCol(cols, modifyColName);
				int lastIndex = cols.size() - 1;
				String afterColName = StringUtil.removeBackquote(((SQLIdentifierExpr) afterColumn).getName());
				//last column is GLOBAL_TABLE_MYCAT_COLUMN,so new add will move to the pos before it
				if (afterColName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)
						&& cols.get(lastIndex).equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
					newModifyColumn.setAfterColumn(new SQLIdentifierExpr(cols.get(lastIndex - 1)));
					cols.add(lastIndex, modifyColName);
				} else {
					addAfterCol(cols, afterColName, modifyColName);
				}
			} else if (modifyColumn.isFirst()) {
				removeOldCol(cols, modifyColName);
				cols.add(0, modifyColName);
			}
		}
		newAlterItems.add(newModifyColumn);
	}

	private void removeOldCol(List<String> cols, String oldColName) {
		for (int i = 0; i < cols.size(); i++) {
			if (cols.get(i).equalsIgnoreCase(oldColName)) {
				cols.remove(i);
				break;
			}
		}
	}

	private void addAfterCol(List<String> cols, String afterColName, String newColName) {
		for (int i = 0; i < cols.size(); i++) {
			if (cols.get(i).equalsIgnoreCase(afterColName)) {
				cols.add(i + 1, newColName);
				break;
			}
		}
	}
	private void changeColumn(List<String> cols, MySqlAlterTableChangeColumn changeColumn, List<SQLAlterTableItem> newAlterItems) {
		String oldColName = StringUtil.removeBackquote(changeColumn.getColumnName().getSimpleName());
		String newColName = StringUtil.removeBackquote(changeColumn.getNewColumnDefinition().getName().getSimpleName());
		MySqlAlterTableChangeColumn newChangeColumn = changeColumn;
		if (oldColName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
			removeOldCol(cols, oldColName);
			newChangeColumn.setNewColumnDefinition(GlobalTableUtil.createMycatColumn());
			newChangeColumn.setAfterColumn(new SQLIdentifierExpr(cols.get(cols.size() - 1)));
			cols.add(newColName);
		} else {
			SQLExpr afterColumn = changeColumn.getAfterColumn();
			if (afterColumn != null) {
				//last column is GLOBAL_TABLE_MYCAT_COLUMN,so new add will move to the pos before it
				removeOldCol(cols, oldColName);
				int lastIndex = cols.size() - 1;
				String afterColName = StringUtil.removeBackquote(((SQLIdentifierExpr) afterColumn).getName());
				if (afterColName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)
						&& cols.get(lastIndex).equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
					newChangeColumn.setAfterColumn(new SQLIdentifierExpr(cols.get(lastIndex-1)));
					cols.add(lastIndex, newColName);
				} else {
					addAfterCol(cols, afterColName, newColName);
				}
			} else if (changeColumn.isFirst()) {
				removeOldCol(cols, oldColName);
				cols.add(0, newColName);
			}
		}
		newAlterItems.add(newChangeColumn);
	}

	private void addColumn(List<String> cols, SQLAlterTableAddColumn addColumn, List<SQLAlterTableItem> newAlterItems) {
		SQLName afterColumn = addColumn.getAfterColumn();
		if (afterColumn != null || addColumn.isFirst()) {
			SQLAlterTableAddColumn newAddColumn = addColumn;
			String addName = StringUtil.removeBackquote(addColumn.getColumns().get(0).getName().getSimpleName());
			if (addName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
				newAddColumn.setFirst(false);
				newAddColumn.setAfterColumn(null);
				cols.add(cols.size(), addName);
			} else if (afterColumn != null) {
				String afterColName = StringUtil.removeBackquote(afterColumn.getSimpleName());
				//last column is GLOBAL_TABLE_MYCAT_COLUMN,so new add will move to the pos before it
				int lastIndex = cols.size() - 1;
				if (afterColName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN) 
						&& cols.get(lastIndex).equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) { 
					newAddColumn.setAfterColumn(new SQLIdentifierExpr(cols.get(lastIndex-1)));
					cols.add(lastIndex, addName);
				}
			} else if (addColumn.isFirst()) {
				cols.add(0, addName);
			}
			newAlterItems.add(newAddColumn);
		} else {
			for (SQLColumnDefinition columnDef : addColumn.getColumns()) {
				SQLAlterTableAddColumn newAddColumn = new SQLAlterTableAddColumn();
				newAddColumn.addColumn(columnDef);
				String addName = StringUtil.removeBackquote(columnDef.getName().getSimpleName());
				int lastIndex = cols.size()-1;
				if (cols.get(lastIndex).equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN)) {
					newAddColumn.setAfterColumn(new SQLIdentifierExpr(cols.get(lastIndex-1)));
					cols.add(lastIndex, addName);
				}else{
					cols.add(addName);
				}
				newAlterItems.add(newAddColumn);
			}
		}
	}
}
