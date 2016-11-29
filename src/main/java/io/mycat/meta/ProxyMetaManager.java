package io.mycat.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.mycat.MycatServer;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.meta.table.SchemaMetaHandler;

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

//	private void dropTable(String schema, String tbName) {
//		SchemaMeta schemaMeta = catalogs.get(schema);
//		if (schemaMeta != null)
//			schemaMeta.dropTable(tbName);
//	}

	

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

//	/**
//	 * 有sql语句执行了,需要查看是否有DDL语句
//	 * 
//	 * @param sql
//	 */
//	// TODO
//	public void noticeSql(String schema, String sql, ClientConnection c) {
//		// 是否是ddl语句，是ddl语句的okresponse则需要通知tmmanager
//		try {
//			SQLStatement ast = new SQLParserDelegate().parse(sql);
//			if (ast instanceof DDLStatement) {
//				logger.info("current ddl sql is :" + sql);
//				if (ast instanceof DDLCreateDatabaseStatement) {
//					createDatabase((DDLCreateDatabaseStatement) ast);
//				} else if (ast instanceof DDLDropDatabaseStatement) {
//					dropDatabase((DDLDropDatabaseStatement) ast);
//				} else if (ast instanceof DDLCreateTableStatement) {
//					createTable(schema, (DDLCreateTableStatement) ast);
//				} else if (ast instanceof DDLDropTableStatement) {
//					dropTable(schema, (DDLDropTableStatement) ast);
//				} else if (ast instanceof DDLTruncateTableStatement) {
//					truncateTable(schema, (DDLTruncateTableStatement) ast);
//				} else if (ast instanceof DDLAlterTableStatement) {
//					alterTable(schema, (DDLAlterTableStatement) ast);
//				} else {
//					// to do further
//				}
//			}
//		} catch (SQLSyntaxErrorException e) {
//			// ignore
//		} finally {
//			c.unlockTable();
//		}
//
//	}

//	private synchronized void createDatabase(DDLCreateDatabaseStatement ast) {
//		String schema = ast.getDatabase().getIdTextUnescape();
//		createDatabase(schema);
//		String path = DATA_ROOT_PATH + "/" + schema;
//		if (zkClientDao.exists(path))
//			return;
//		zkClientDao.create(path, null, CreateMode.PERSISTENT);
//		String tblPath = path + "/table";
//		zkClientDao.create(tblPath, null, CreateMode.PERSISTENT);
//		String viewPath = path + "/view";
//		zkClientDao.create(viewPath, null, CreateMode.PERSISTENT);
//	}
//
//	private synchronized void dropDatabase(DDLDropDatabaseStatement ast) {
//		String schema = ast.getDatabase().getIdTextUnescape();
//		dropDatabase(schema);
//		String path = DATA_ROOT_PATH + "/" + schema;
//		if (zkClientDao.exists(path))
//			zkClientDao.deleteRecursive(path);
//	}
//
//	private synchronized void createTable(String schema, DDLCreateTableStatement ast) {
//		Identifier tbIdentifier = ast.getTable();
//		if (tbIdentifier.getParent() != null)
//			schema = tbIdentifier.getParent().getIdTextUnescape();
//		String tbName = ast.getTable().getIdTextUnescape();
//		boolean ifNotExist = ast.isIfNotExists();
//		if (ifNotExist && containTable(schema, tbName)) {
//			return;
//		}
//
//		TableMeta.Builder tmBuilder = TableMeta.newBuilder();
//		List<ColumnMeta> columnMetas = new LinkedList<ColumnMeta>();
//		boolean hasAutoIncrement = false;
//		if (ast.getCreateDefs() != null) {
//			for (int i = 0; i < ast.getCreateDefs().size(); i++) {
//				CreateDefinition createDef = ast.getCreateDefs().get(i);
//				if (createDef.getColName() != null) {
//					String colName = createDef.getColName().getIdTextUnescape();
//					ColumnDefinition columnDef = createDef.getColDef();
//					ColumnMeta colMeta = constructColumnMeta(tbName, colName, columnDef);
//					if (colMeta.getAutoIncre()) {
//						tmBuilder.setAiColPos(columnMetas.size());
//						hasAutoIncrement = true;
//					}
//					columnMetas.add(colMeta);
//				}
//			}
//		}
//		tmBuilder.setCatalog(schema).setTableName(tbName).addAllAllColumns(columnMetas);
//		long offset = -1;
//		if (ast.getTableOptions() != null && ast.getTableOptions().getAutoIncrement() != null) {
//			Object obj = ast.getTableOptions().getAutoIncrement()
//					.evaluation(ExprEvaluationUtil.getEvaluationParameters());
//			if (obj instanceof Number) {
//				offset = ((Number) obj).longValue();
//			}
//			tmBuilder.setAiOffset(offset);
//		}
//		long version = System.currentTimeMillis();
//		tmBuilder.setVersion(version);
//		TableMeta tblMeta = tmBuilder.build();
//		if (hasAutoIncrement) {
//			ProxyServer.getInstance().addSequence(schema, tbName, offset);
//		}
//		if (!standalone) {
//			String path = DATA_ROOT_PATH + "/" + schema + "/table/" + tbName;
//			zkClientDao.create(path, tblMeta.toByteArray(), CreateMode.PERSISTENT);
//		}
//		addTable(schema, tblMeta);
//	}
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
//	private synchronized void dropTable(String schema, DDLDropTableStatement ast) {
//		for (Identifier tbIndentifier : ast.getTableNames()) {
//			if (tbIndentifier.getParent() != null)
//				schema = tbIndentifier.getParent().getIdTextUnescape();
//			String table = tbIndentifier.getIdTextUnescape();
//			dropTable(schema, table);
//			if (!standalone) {
//				String path = DATA_ROOT_PATH + "/" + schema + "/table/" + table;
//				if (zkClientDao.exists(path))
//					zkClientDao.delete(path);
//			}
//			ProxyServer.getInstance().removeSequence(schema, table);
//		}
//	}

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

//	private synchronized void alterTable(String schema, DDLAlterTableStatement ast) {
//		if (ast.getTable().getParent() != null)
//			schema = ast.getTable().getParent().getIdTextUnescape();
//		String table = ast.getTable().getIdTextUnescape();
//		TableMeta orgTbMeta = getTableMeta(schema, table);
//		if (orgTbMeta == null)
//			return;
//		TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
//		List<ColumnMeta> cols = new ArrayList<ColumnMeta>();
//		cols.addAll(orgTbMeta.getAllColumnsList());
//		for (AlterSpecification alter : ast.getAlters()) {
//			if (alter instanceof AddColumn) {
//				addColumn(orgTbMeta, cols, (AddColumn) alter);
//			} else if (alter instanceof AddColumns) {
//				addColumns(orgTbMeta, cols, (AddColumns) alter);
//			} else if (alter instanceof ChangeColumn) {
//				changeColumn(orgTbMeta, cols, (ChangeColumn) alter);
//			} else if (alter instanceof DropColumn) {
//				dropColumn(cols, (DropColumn) alter);
//			} else if (alter instanceof ModifyColumn) {
//				modifyColumn(orgTbMeta, cols, (ModifyColumn) alter);
//			} else {
//				// to do fur
//			}
//		}
//		tmBuilder.clearAllColumns().addAllAllColumns(cols);
//		long offset = -1;
//		if (ast.getTableOptions() != null && ast.getTableOptions().getAutoIncrement() != null) {
//			Object obj = ast.getTableOptions().getAutoIncrement()
//					.evaluation(ExprEvaluationUtil.getEvaluationParameters());
//			if (obj instanceof Number) {
//				offset = ((Number) obj).longValue();
//			}
//			tmBuilder.setAiOffset(offset);
//		}
//		long version = System.currentTimeMillis();
//		tmBuilder.setVersion(version);
//		tmBuilder.setType(1);
//		TableMeta newTblMeta = tmBuilder.build();
//		/* add for view to rebuild */
//		onTableChange(schema, table);
//		/* autoIncrement change */
//		if (offset != -1) {
//			ProxyServer.getInstance().addSequence(schema, table, offset);
//		}
//		if (!standalone) {
//			String path = DATA_ROOT_PATH + "/" + schema + "/table/" + table;
//			zkClientDao.writeData(path, newTblMeta.toByteArray());
//		}
//		addTable(schema, newTblMeta);
//	}

	
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

//	private void addColumn(TableMeta tbMeta, List<ColumnMeta> columnMetas, AddColumn addColumn) {
//		String addColName = addColumn.getColumnName().getIdTextUnescape();
//		if (tbMeta.getColumn(addColName) == null) {
//			ColumnMeta colMeta = constructColumnMeta(tbMeta.getTableName(), addColName, addColumn.getColumnDefine());
//			int index = -1;
//			if (addColumn.isFirst()) {
//				index = 0;
//			} else if (addColumn.getAfterColumn() != null) {
//				String afterColName = addColumn.getAfterColumn().getIdTextUnescape();
//				for (int i = 0; i < columnMetas.size(); i++) {
//					String colName = columnMetas.get(i).getName();
//					if (afterColName.equalsIgnoreCase(colName)) {
//						index = i + 1;
//						break;
//					}
//				}
//				if (index == -1) {
//					throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "42S22",
//							String.format("Unknown column '%s' in '%s'", afterColName, tbMeta.getTableName()));
//				}
//			} else {
//				index = columnMetas.size();
//			}
//			columnMetas.add(index, colMeta);
//		}
//	}
//
//	private void addColumns(TableMeta tbMeta, List<ColumnMeta> columnMetas, AddColumns addColumns) {
//		for (Pair<Identifier, ColumnDefinition> pair : addColumns.getColumns()) {
//			String colName = pair.getKey().getIdTextUnescape();
//			if (tbMeta.getColumn(colName) == null) {
//				ColumnMeta colMeta = constructColumnMeta(tbMeta.getTableName(), colName, pair.getValue());
//				columnMetas.add(colMeta);
//			}
//		}
//	}
//
//	private ColumnMeta constructColumnMeta(String tbName, String colName, ColumnDefinition columnDef) {
//		ColumnMeta.Builder colBuilder = ColumnMeta.newBuilder();
//		colBuilder.setTableName(tbName).setName(colName);
//		if (columnDef == null) {
//			return colBuilder.build();
//		}
//		colBuilder.setCanNull(!columnDef.isNotNull());
//		if (columnDef.getSpecialIndex() != null) {
//			switch (columnDef.getSpecialIndex()) {
//			case PRIMARY:
//				colBuilder.setKey("PRI");
//				break;
//			case UNIQUE:
//				colBuilder.setKey("UNI");
//				break;
//			}
//		}
//		if (columnDef.getDefaultValue() != null) {
//			StringBuilder builder = new StringBuilder();
//			MySQLOutputASTVisitor visitor = new MySQLOutputASTVisitor(builder);
//			columnDef.getDefaultValue().accept(visitor);
//			colBuilder.setSdefault(builder.toString());
//		}
//		colBuilder.setAutoIncre(columnDef.isAutoIncrement());
//		return colBuilder.build();
//	}
//
//	private void changeColumn(TableMeta tbMeta, List<ColumnMeta> columnMetas, ChangeColumn changeColumn) {
//		String oldColName = changeColumn.getOldName().getIdTextUnescape();
//		String newColName = changeColumn.getNewName().getIdTextUnescape();
//		ColumnMeta colMeta = constructColumnMeta(tbMeta.getTableName(), newColName, changeColumn.getColDef());
//		int index = -1;
//		for (int i = 0; i < columnMetas.size(); i++) {
//			String colName = columnMetas.get(i).getName();
//			if (oldColName.equalsIgnoreCase(colName)) {
//				index = i;
//				columnMetas.remove(i);
//				break;
//			}
//		}
//
//		if (index == -1) {
//			throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "42S22",
//					String.format("Unknown column '%s' in '%s'", oldColName, tbMeta.getTableName()));
//		}
//
//		if (changeColumn.isFirst()) {
//			index = 0;
//		} else if (changeColumn.getAfterColumn() != null) {
//			index = -1;
//			String afterColName = changeColumn.getAfterColumn().getIdTextUnescape();
//			for (int i = 0; i < columnMetas.size(); i++) {
//				String colName = columnMetas.get(i).getName();
//				if (afterColName.equalsIgnoreCase(colName)) {
//					index = i + 1;
//					break;
//				}
//			}
//
//			if (index == -1) {
//				throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "42S22",
//						String.format("Unknown column '%s' in '%s'", afterColName, tbMeta.getTableName()));
//			}
//		}
//		columnMetas.add(index, colMeta);
//	}
//
//	private void dropColumn(List<ColumnMeta> columnMetas, DropColumn dropColumn) {
//		String dropColName = dropColumn.getColName().getIdTextUnescape();
//		for (int i = 0; i < columnMetas.size(); i++) {
//			String colName = columnMetas.get(i).getName();
//			if (dropColName.equalsIgnoreCase(colName)) {
//				columnMetas.remove(i);
//				return;
//			}
//		}
//	}
//
//	private void modifyColumn(TableMeta tbMeta, List<ColumnMeta> columnMetas, ModifyColumn modifyColumn) {
//		String modifyColName = modifyColumn.getColName().getIdTextUnescape();
//		ColumnMeta colMeta = constructColumnMeta(tbMeta.getTableName(), modifyColName, modifyColumn.getColDef());
//		int index = -1;
//		for (int i = 0; i < columnMetas.size(); i++) {
//			String colName = columnMetas.get(i).getName();
//			if (modifyColName.equalsIgnoreCase(colName)) {
//				index = i;
//				columnMetas.remove(i);
//				break;
//			}
//		}
//
//		if (index == -1) {
//			throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "42S22",
//					String.format("Unknown column '%s' in '%s'", modifyColName, tbMeta.getTableName()));
//		}
//
//		if (modifyColumn.isFirst()) {
//			index = 0;
//		} else if (modifyColumn.getAfterColumn() != null) {
//			index = -1;
//			String afterColName = modifyColumn.getAfterColumn().getIdTextUnescape();
//			for (int i = 0; i < columnMetas.size(); i++) {
//				String colName = columnMetas.get(i).getName();
//				if (afterColName.equalsIgnoreCase(colName)) {
//					index = i + 1;
//					break;
//				}
//			}
//
//			if (index == -1) {
//				throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "42S22",
//						String.format("Unknown column '%s' in '%s'", modifyColName, tbMeta.getTableName()));
//			}
//		}
//
//		columnMetas.add(index, colMeta);
//	}

//	private void onTableChange(String schema, String tbName) {
//		// TODO
//	}

	
}
