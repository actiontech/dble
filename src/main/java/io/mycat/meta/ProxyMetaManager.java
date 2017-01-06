package io.mycat.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.protocol.MyCatMeta.ColumnMeta;
import io.mycat.meta.protocol.MyCatMeta.IndexMeta;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.meta.table.AbstractTableMetaHandler;
import io.mycat.meta.table.MetaHelper;
import io.mycat.meta.table.MetaHelper.INDEX_TYPE;
import io.mycat.meta.table.SchemaMetaHandler;
import io.mycat.meta.table.TableMetaCheckHandler;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.util.StringUtil;

public class ProxyMetaManager {
	/* catalog,table,tablemeta */
	private final Map<String, SchemaMeta> catalogs;

	public ProxyMetaManager() {
		this.catalogs = new ConcurrentHashMap<String, SchemaMeta>();
	}

	public Map<String, SchemaMeta> getCatalogs() {
		return catalogs;
	}

	public SchemaMeta getSchema(String schema) {
		if (schema == null)
			return null;
		return this.catalogs.get(schema);
	}

	/**
	 * synchronously getting schemas from cluster. just for show databases
	 * command
	 * 
	 * @return
	 */
	public synchronized Map<String, SchemaMeta> getSchemas() {
		return catalogs;
	}

	/**
	 * @param schema
	 * @return 是否真实create
	 */
	public boolean createDatabase(String schema) {
		SchemaMeta schemaMeta = catalogs.get(schema);
		if (schemaMeta == null) {
			schemaMeta = new SchemaMeta();
			catalogs.put(schema, schemaMeta);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checking the existence of the database in which the view is to be created
	 * 
	 * @param db
	 * @return
	 */
	public boolean checkDbExists(String schema) {
		if (schema == null)
			return false;
		return this.catalogs.containsKey(schema);
	}

	public boolean checkTableExists(String schema, String strTable) {
		if(!checkDbExists(schema))
			return false;
		if (strTable == null)
			return false;
		return this.catalogs.get(schema).getTableMetas().containsKey(strTable);
	}

	public List<String> getTableNames(String schema) {
		List<String> tbNames;
		SchemaMeta schemaMeta = catalogs.get(schema);
		if (schemaMeta == null)
			return new ArrayList<String>();
		tbNames = schemaMeta.getTables();
		Collections.sort(tbNames);
		return tbNames;
	}

	public void dropDatabase(String schema) {
		catalogs.remove(schema);
	}

	public void addTable(String schema, TableMeta tm) {
		String tbName = tm.getTableName();
		SchemaMeta schemaMeta = catalogs.get(schema);
		if (schemaMeta != null) {
			schemaMeta.addTableMeta(tbName, tm);
		}
	}

	public boolean flushTable(String schema, TableMeta tm) {
		String tbName = tm.getTableName();
		SchemaMeta schemaMeta = catalogs.get(schema);
		if (schemaMeta != null) {
			TableMeta oldTm = schemaMeta.addTableMetaIfAbsent(tbName, tm);
			if (oldTm != null) {
				TableMeta tblMetaTmp = tm.toBuilder().setVersion(oldTm.getVersion()).build();
				if (!oldTm.equals(tblMetaTmp)) {
					return schemaMeta.flushTableMeta(tbName, oldTm, tm);
				}
			}
		}
		return true;
	}

	private void dropTable(String schema, String tbName) {
		SchemaMeta schemaMeta = catalogs.get(schema);
		if (schemaMeta != null)
			schemaMeta.dropTable(tbName);
	}

	public TableMeta getTableMeta(String schema, String tbName) {
		return catalogs.get(schema).getTableMeta(tbName);
	}
 
	

	public TableMeta removeTableMetaNosync(String schema, String tbName) {
		SchemaMeta schemaMeta = catalogs.remove(schema);
		if (schemaMeta == null) {
			return null;
		}
		return schemaMeta.dropTable(tbName);
	}
 
//	public void createView(String schema, String viewName, String createSql, List<String> columns, PlanNode selectNode,
//			ViewCreateMode mode, boolean writeToZk) {
//		if (!checkDbExists(schema)) {
//			throw new MySQLOutPutException(1049, "42000", String.format("Unknown database '%s'", schema));
//		}
//		SchemaMeta schemaMeta = getSchema(schema);
//		if (mode == ViewCreateMode.VIEW_CREATE_NEW && schemaMeta.getViewMeta(viewName) != null)
//			throw new MySQLOutPutException(1050, "42S01", String.format("Table '%s' already exists", viewName));
//		if (mode == ViewCreateMode.VIEW_ALTER && schemaMeta.getViewMeta(viewName) == null)
//			throw new MySQLOutPutException(1146, "42S02", String.format("Table '%s' doesn't exist", viewName));
//		// see mysql_create_view.cc for more check to do
//		selectNode.setUpFields();
//		selectNode = SubQueryPreProcessor.optimize(selectNode);
//		/* view list (list of view fields names) */
//		if (columns != null && columns.size() > 0) {
//			List<Item> sels = selectNode.getColumnsSelected();
//			if (sels.size() != columns.size()) {
//				throw new MySQLOutPutException(1353, "HY000",
//						String.format("View's SELECT and view's field list have different column counts"));
//			}
//			for (int index = 0; index < columns.size(); index++) {
//				Item sel = sels.get(index);
//				String columnName = columns.get(index);
//				if (!StringUtils.equals(sel.getItemName(), columnName))
//					sel.setAlias(columnName);
//			}
//		}
//		ViewMeta vm = new ViewMeta(new ViewNode(schema, viewName, selectNode, createSql));
//		String path = DATA_ROOT_PATH + "/" + schema + "/view/" + viewName;
//		byte[] data = null;
//		try {
//			data = createSql.getBytes("UTF-8");
//		} catch (UnsupportedEncodingException e) {
//			// ignore
//		}
//		if (writeToZk) {
//			switch (mode) {
//			case VIEW_CREATE_NEW:
//				zkClientDao.create(path, data, CreateMode.PERSISTENT);
//				break;
//			case VIEW_CREATE_OR_REPLACE:
//				if (!zkClientDao.exists(path)) {
//					zkClientDao.create(path, data, CreateMode.PERSISTENT);
//				} else {
//					zkClientDao.writeData(path, data);
//				}
//				break;
//			default:
//				zkClientDao.writeData(path, data);
//				break;
//			}
//		}
//		schemaMeta.addViewMeta(viewName, vm);
//	}

//	@Override
//	public void dropView(List<Pair<String, String>> views, boolean ifExists) {
//		StringBuilder unknownTable = new StringBuilder();
//		boolean isFirst = true;
//		for (Pair<String, String> view : views) {
//			String schema = view.getKey();
//			String viewName = view.getValue();
//			if (!containsView(schema, viewName) && ifExists == false) {
//				if (isFirst) {
//					isFirst = false;
//				} else {
//					unknownTable.append(",");
//				}
//				unknownTable.append(SqlMaker.getFullName(schema, viewName));
//			} else {
//				SchemaMeta schemaMeta = getSchema(schema);
//				String path = DATA_ROOT_PATH + "/" + schema + "/view/" + viewName;
//				schemaMeta.dropView(viewName);
//				zkClientDao.delete(path);
//			}
//
//		}
//		if (unknownTable.length() > 0) {
//			throw new MySQLOutPutException(1051, "42S02", String.format("Unknown table '%s'", unknownTable.toString()));
//		}
//	}
//
//	public boolean containsViewNoSync(String schema, String viewName) {
//		SchemaMeta schemaMeta = this.catalogs.get(schema);
//		if (schemaMeta == null)
//			return false;
//		return schemaMeta.containsView(viewName);
//	}
//
//	public boolean containsView(String schema, String viewName) {
//		if (!checkDbExists(schema)) {
//			return false;
//		} else {
//			String path = DATA_ROOT_PATH + "/" + schema + "/view/" + viewName;
//			boolean zkExisted = zkClientDao.exists(path);
//			boolean rst = getSchema(schema).containsView(viewName);
//			if (zkExisted != rst) {
//				if (zkExisted) {
//					try {
//						byte[] data = zkClientDao.readData(path);
//						String sql = new String(data, "UTF-8");
//						compileView(schema, sql);
//					} catch (UnsupportedEncodingException e) {
//						// ignore
//					}
//				} else {
//					getSchema(schema).dropView(viewName);
//				}
//			}
//			return zkExisted;
//		}
//	}
//
//	public void compileView(String schema, String sql) {
//		try {
//			SQLStatement stmt = new SQLParserDelegate().parse(sql);
//			MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(schema);
//			visitor.setWriteToZk(false);
//			stmt.accept(visitor);
//		} catch (Exception e) {
//			logger.debug("compile view for [" + sql + "] error:", e);
//		}
//	}
//
//	/**
//	 * need call contains first
//	 * 
//	 * @param user
//	 * @param schema
//	 * @param viewName
//	 * @return
//	 */
//	public String getViewSQL(String schema, String viewName) {
//		SchemaMeta schemaMeta = getSchema(schema);
//		if (schemaMeta == null)
//			return null;
//		ViewMeta viewMeta = schemaMeta.getViewMeta(viewName);
//		if (viewMeta == null)
//			return null;
//		return viewMeta.getCreateSql();
//	}
//
//	@Override
//	public ViewNode getView(String schema, String viewName) {
//		if (!containsViewNoSync(schema, viewName))
//			return null;
//		SchemaMeta schemaMeta = getSchema(schema);
//		ViewMeta vm = schemaMeta.getViewMeta(viewName);
//		return vm.getViewNode();
//	}

	public void init()  {
		SchemaMetaHandler handler = new SchemaMetaHandler(MycatServer.getInstance().getConfig());
		handler.execute();
	}

	public synchronized void tableStructureCheck() {
		for (SchemaConfig schema : MycatServer.getInstance().getConfig().getSchemas().values()) {
			if (!checkDbExists(schema.getName())) {
				continue;
			}
			for (TableConfig table : schema.getTables().values()) {
				if (!checkTableExists(schema.getName(), table.getName())) {
					continue;
				}
				AbstractTableMetaHandler handler = new TableMetaCheckHandler(schema.getName(), table);
				handler.execute();
			}
		}
	}

	/**
	 * 有sql语句执行了,需要查看是否有DDL语句
	 * 
	 * @param sql
	 */
	public void noticeSql(String schema, String sql) {
		// 是否是ddl语句，是ddl语句的okresponse则需要通知tmmanager
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLStatement statement = parser.parseStatement();
		if (statement instanceof MySqlCreateTableStatement) {
			createTable(schema, (MySqlCreateTableStatement) statement);
		} else if (statement instanceof SQLDropTableStatement) {
			dropTable(schema, (SQLDropTableStatement) statement);
		} else if (statement instanceof SQLAlterTableStatement) {
			alterTable(schema, (SQLAlterTableStatement) statement);
		} else if (statement instanceof SQLTruncateStatement) {
			// TODO:Sequence?
		} else if (statement instanceof SQLCreateIndexStatement) {
			createIndex(schema, (SQLCreateIndexStatement) statement);
		} else if (statement instanceof SQLDropIndexStatement) {
			dropIndex(schema, (SQLDropIndexStatement) statement);
		} else {
			// TODO: further
		}
//		finally {
//			c.unlockTable();
//		}
	}

	private synchronized void createTable(String schema, MySqlCreateTableStatement statement) {
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, statement.getTableSource());
		TableMeta tblMeta = MetaHelper.initTableMeta(schemaInfo.table, statement, System.currentTimeMillis());
		addTable(schemaInfo.schema, tblMeta);
	}

	private synchronized void dropTable(String schema, SQLDropTableStatement statement) {
		for (SQLExprTableSource table : statement.getTableSources()) {
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, table);
			dropTable(schemaInfo.schema, schemaInfo.table);
		}
	}

//
//	private synchronized void truncateTable(String schema, DDLTruncateTableStatement ast) {
//		String table = ast.getTable().getIdTextUnescape();
//		TableMeta tbMeta = getTableMeta(schema, table);
//		if (tbMeta == null)
//			return;
//		TableMeta.Builder tmBuilder = tbMeta.toBuilder();
//		long version = System.currentTimeMillis();
//		tmBuilder.setVersion(version);
//		tbMeta = tmBuilder.build();
//		if (ProxyServer.getInstance().removeSequence(schema, table)) {
//			long offset = tbMeta.getAiOffset();
//			ProxyServer.getInstance().addSequence(schema, table, offset);
//		}
//		if (!standalone) {
//			String path = DATA_ROOT_PATH + "/" + schema + "/table/" + table;
//			zkClientDao.writeData(path, tbMeta.toByteArray());
//		}
//		addTable(schema, tbMeta);
//	}
//


//	
//	public void renameTable(String schema, DDLRenameTableStatement ast) {
//		for (Pair<Identifier, Identifier> pair : ast.getList()) {
//			String orgSchema = schema;
//			String newSchema = schema;
//			if (pair.getKey().getParent() != null)
//				orgSchema = pair.getKey().getParent().getIdTextUnescape();
//			if (pair.getValue().getParent() != null)
//				newSchema = pair.getValue().getParent().getIdTextUnescape();
//			String orgTable = pair.getKey().getIdTextUnescape();
//			String newTable = pair.getValue().getIdTextUnescape();
//			renameTable(orgSchema, orgTable, newSchema, newTable);
//		}
//	}

	private synchronized void alterTable(String schema, SQLAlterTableStatement alterStatement) {
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, alterStatement.getTableSource());
		TableMeta orgTbMeta = getTableMeta(schemaInfo.schema, schemaInfo.table);
		if (orgTbMeta == null)
			return;
		TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
		List<ColumnMeta> cols = new ArrayList<ColumnMeta>();
		cols.addAll(orgTbMeta.getColumnsList());
		int autoColumnIndex = -1;
		for (SQLAlterTableItem alterItem : alterStatement.getItems()) {
			if (alterItem instanceof SQLAlterTableAddColumn) {
				autoColumnIndex = addColumn(cols, (SQLAlterTableAddColumn) alterItem);
			} else if (alterItem instanceof SQLAlterTableAddIndex) {
				addIndex(tmBuilder, (SQLAlterTableAddIndex) alterItem);
			} else if (alterItem instanceof SQLAlterTableAddConstraint) {
				SQLAlterTableAddConstraint addConstraint = (SQLAlterTableAddConstraint) alterItem;
				SQLConstraint constraint = addConstraint.getConstraint();
				if (constraint instanceof MySqlPrimaryKey) {
					MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) constraint;
					IndexMeta indexMeta = MetaHelper.makeIndexMeta(MetaHelper.PRIMARY, MetaHelper.INDEX_TYPE.PRI, primaryKey.getColumns());
					tmBuilder.setPrimary(indexMeta);
				} else {// NOT SUPPORT
				}
			} else if (alterItem instanceof SQLAlterTableDropIndex) {
				SQLAlterTableDropIndex dropIndex = (SQLAlterTableDropIndex) alterItem;
				String dropName = StringUtil.removeBackquote(dropIndex.getIndexName().getSimpleName());
				dropIndex(tmBuilder, dropName);
			} else if (alterItem instanceof SQLAlterTableDropKey) {
				SQLAlterTableDropKey dropIndex = (SQLAlterTableDropKey) alterItem;
				String dropName = StringUtil.removeBackquote(dropIndex.getKeyName().getSimpleName());
				dropIndex(tmBuilder, dropName);
			} else if (alterItem instanceof MySqlAlterTableChangeColumn) {
				autoColumnIndex = changeColumn(cols, (MySqlAlterTableChangeColumn) alterItem);
			} else if (alterItem instanceof MySqlAlterTableModifyColumn) {
				autoColumnIndex = modifyColumn(cols, (MySqlAlterTableModifyColumn) alterItem);
			} else if (alterItem instanceof SQLAlterTableDropColumnItem) {
				dropColumn(cols, (SQLAlterTableDropColumnItem) alterItem);
			} else if (alterItem instanceof SQLAlterTableDropPrimaryKey) {
				tmBuilder.clearPrimary();
			} else {
				// TODO: further
			}
		}
		tmBuilder.clearColumns().addAllColumns(cols);
//		long offset = -1;
//		if (ast.getTableOptions() != null && ast.getTableOptions().getAutoIncrement() != null) {
//			Object obj = ast.getTableOptions().getAutoIncrement()
//					.evaluation(ExprEvaluationUtil.getEvaluationParameters());
//			if (obj instanceof Number) {
//				offset = ((Number) obj).longValue();
//			}
//			tmBuilder.setAiOffset(offset);
//		}
		if (autoColumnIndex != -1) {
			tmBuilder.setAiColPos(autoColumnIndex);
		}
		tmBuilder.setVersion(System.currentTimeMillis());
		TableMeta newTblMeta = tmBuilder.build();
		addTable(schema, newTblMeta);
	}
	private synchronized void createIndex(String schema, SQLCreateIndexStatement stament){
		SQLTableSource tableSource = stament.getTable();
		if (tableSource instanceof SQLExprTableSource) {
			SQLExprTableSource exprTableSource = (SQLExprTableSource)tableSource;
			SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, exprTableSource);
			TableMeta orgTbMeta = getTableMeta(schemaInfo.schema, schemaInfo.table);
			if (orgTbMeta == null)
				return;
			String indexName = stament.getName().getSimpleName();
			TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
			if (stament.getType() == null) {
				addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.MUL, stament.getItems());
			} else if (stament.getType().equals("UNIQUE")) {
				addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.UNI, stament.getItems());
			}
		}
	}
	private void addIndex(TableMeta.Builder tmBuilder, SQLAlterTableAddIndex addIndex) {
		String indexName = addIndex.getName().getSimpleName();
		if (addIndex.isUnique()) {
			addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.UNI, addIndex.getItems());
		} else {
			addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.MUL, addIndex.getItems());
		}
	}

	private void addIndex(String indexName, TableMeta.Builder tmBuilder, INDEX_TYPE indexType, List<SQLSelectOrderByItem> items){
		List<SQLExpr> columnExprs = new ArrayList<SQLExpr>();
		for (SQLSelectOrderByItem item :items) {
			columnExprs.add(item.getExpr());
		}
		IndexMeta indexMeta = MetaHelper.makeIndexMeta(indexName, indexType, columnExprs);
		tmBuilder.addIndex(indexMeta);
	}
	private synchronized void dropIndex(String schema, SQLDropIndexStatement dropIndexStatement){
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schema, dropIndexStatement.getTableName());
		TableMeta orgTbMeta = getTableMeta(schemaInfo.schema, schemaInfo.table);
		if (orgTbMeta == null)
			return;
		TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
		String dropName = StringUtil.removeBackquote(((SQLIdentifierExpr) dropIndexStatement.getIndexName()).getName());
		dropIndex(tmBuilder, dropName);
	}
	private void dropIndex(TableMeta.Builder tmBuilder, String dropName) {
		List<IndexMeta> indexs = new ArrayList<IndexMeta>();
		indexs.addAll(tmBuilder.getIndexList());
		if (dropIndex(indexs, dropName)) {
			tmBuilder.clearIndex().addAllIndex(indexs);
		} else {
			List<IndexMeta> uniques = new ArrayList<IndexMeta>();
			uniques.addAll(tmBuilder.getUniIndexList());
			dropIndex(uniques, dropName);
			tmBuilder.clearUniIndex().addAllUniIndex(uniques);
		}
	}

	private boolean dropIndex(List<IndexMeta> indexs, String dropName) {
		int index = -1;
		for (int i = 0; i < indexs.size(); i++) {
			String indexName = indexs.get(i).getName();
			if (indexName.equalsIgnoreCase(dropName)) {
				index = i;
				break;
			}
		}
		if (index != -1) {
			indexs.remove(index);
			return true;
		}
		return false;
	}
//	private void renameTable(String orgSchema, String orgTable, String newSchema, String newTable) {
//		TableMeta orgTbMeta = removeTableMetaNosync(orgSchema, orgTable);
//		if (orgTbMeta == null)
//			return;
//		List<ColumnMeta> orgCols = orgTbMeta.getAllColumnsList();
//		List<ColumnMeta> newCols = new ArrayList<ColumnMeta>();
//		for (ColumnMeta orgCol : orgCols) {
//			ColumnMeta.Builder cmBuilder = orgCol.toBuilder();
//			cmBuilder.setTableName(newTable);
//			newCols.add(cmBuilder.build());
//		}
//		TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
//		tmBuilder.setCatalog(newSchema).setTableName(newTable).clearAllColumns().addAllAllColumns(newCols);
//		addTable(newSchema, tmBuilder.build());
//	}

	private int addColumn(List<ColumnMeta> columnMetas, SQLAlterTableAddColumn addColumn) {
		int autoColumnIndex = -1;
		boolean isFirst = addColumn.isFirst();
		SQLName afterColumn = addColumn.getAfterColumn();
		if (afterColumn != null || isFirst) {
			int addIndex = -1;
			if (isFirst) {
				addIndex = 0;
			} else {
				String afterColName = StringUtil.removeBackquote(afterColumn.getSimpleName());
				for (int i = 0; i < columnMetas.size(); i++) {
					String colName = columnMetas.get(i).getName();
					if (afterColName.equalsIgnoreCase(colName)) {
						addIndex = i + 1;
						break;
					}
				}
			}
			ColumnMeta.Builder cmBuilder = MetaHelper.makeColumnMeta(addColumn.getColumns().get(0));
			columnMetas.add(addIndex, cmBuilder.build());
			if (cmBuilder.getAutoIncre()) {
				autoColumnIndex = addIndex;
			}
		} else {
			int addIndex = columnMetas.size();
			for (SQLColumnDefinition columnDef : addColumn.getColumns()) {
				ColumnMeta.Builder cmBuilder = MetaHelper.makeColumnMeta(columnDef);
				columnMetas.add(addIndex, cmBuilder.build());
				if (cmBuilder.getAutoIncre()) {
					autoColumnIndex = addIndex;
				}
			}
		}
		return autoColumnIndex;
	}

	private int changeColumn(List<ColumnMeta> columnMetas, MySqlAlterTableChangeColumn changeColumn) {
		int autoColumnIndex = -1;
		String changeColName = StringUtil.removeBackquote(changeColumn.getColumnName().getSimpleName());
		for (int i = 0; i < columnMetas.size(); i++) {
			String colName = columnMetas.get(i).getName();
			if (changeColName.equalsIgnoreCase(colName)) {
				columnMetas.remove(i);
				break;
			}
		}
		boolean isFirst = changeColumn.isFirst();
		SQLExpr afterColumn = changeColumn.getAfterColumn();
		int changeIndex = -1;
		if (isFirst) {
			changeIndex = 0;
		} else if (afterColumn != null) {
			String afterColName = StringUtil.removeBackquote(((SQLIdentifierExpr) afterColumn).getName());
			for (int i = 0; i < columnMetas.size(); i++) {
				String colName = columnMetas.get(i).getName();
				if (afterColName.equalsIgnoreCase(colName)) {
					changeIndex = i + 1;
					break;
				}
			}
		} else {
			changeIndex = columnMetas.size();
		}
		ColumnMeta.Builder cmBuilder = MetaHelper.makeColumnMeta(changeColumn.getNewColumnDefinition());
		columnMetas.add(changeIndex, cmBuilder.build());
		if (cmBuilder.getAutoIncre()) {
			autoColumnIndex = changeIndex;
		}
		return autoColumnIndex;
	}

	private void dropColumn(List<ColumnMeta> columnMetas, SQLAlterTableDropColumnItem dropColumn) {
		for (SQLName dropName : dropColumn.getColumns()) {
			String dropColName = StringUtil.removeBackquote(dropName.getSimpleName());
			for (int i = 0; i < columnMetas.size(); i++) {
				String colName = columnMetas.get(i).getName();
				if (dropColName.equalsIgnoreCase(colName)) {
					columnMetas.remove(i);
					break;
				}
			}
		}
	}

	private int modifyColumn(List<ColumnMeta> columnMetas, MySqlAlterTableModifyColumn modifyColumn) {
		int autoColumnIndex = -1;
		SQLColumnDefinition modifyColDef = modifyColumn.getNewColumnDefinition();
		String modifyColName = StringUtil.removeBackquote(modifyColDef.getName().getSimpleName());
		for (int i = 0; i < columnMetas.size(); i++) {
			String colName = columnMetas.get(i).getName();
			if (modifyColName.equalsIgnoreCase(colName)) {
				columnMetas.remove(i);
				break;
			}
		}
		boolean isFirst = modifyColumn.isFirst();
		SQLExpr afterColumn = modifyColumn.getAfterColumn();
		int modifyIndex = -1;
		if (isFirst) {
			modifyIndex = 0;
		} else if (afterColumn != null) {
			String afterColName = StringUtil.removeBackquote(((SQLIdentifierExpr) afterColumn).getName());
			for (int i = 0; i < columnMetas.size(); i++) {
				String colName = columnMetas.get(i).getName();
				if (afterColName.equalsIgnoreCase(colName)) {
					modifyIndex = i + 1;
					break;
				}
			}
		} else {
			modifyIndex = columnMetas.size();
		}
		ColumnMeta.Builder cmBuilder = MetaHelper.makeColumnMeta(modifyColDef);
		columnMetas.add(modifyIndex, cmBuilder.build());
		if (cmBuilder.getAutoIncre()) {
			autoColumnIndex = modifyIndex;
		}
		return autoColumnIndex;
	}

//	private void onTableChange(String schema, String tbName) {
//		// TODO
//	}

	
}
