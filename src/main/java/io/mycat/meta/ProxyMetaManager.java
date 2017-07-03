package io.mycat.meta;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.config.MycatConfig;
import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.zookeeper.process.DDLInfo;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.protocol.StructureMeta.ColumnMeta;
import io.mycat.meta.protocol.StructureMeta.IndexMeta;
import io.mycat.meta.protocol.StructureMeta.TableMeta;
import io.mycat.meta.table.AbstractTableMetaHandler;
import io.mycat.meta.table.MetaHelper;
import io.mycat.meta.table.MetaHelper.INDEX_TYPE;
import io.mycat.meta.table.SchemaMetaHandler;
import io.mycat.meta.table.TableMetaCheckHandler;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.util.StringUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static io.mycat.config.loader.console.ZookeeperPath.FLOW_ZK_PATH_ONLINE;
import static io.mycat.config.loader.zkprocess.zookeeper.process.DDLInfo.DDLStatus;

public class ProxyMetaManager {
	protected static final Logger LOGGER = LoggerFactory.getLogger(ProxyMetaManager.class);
	/* catalog,table,tablemeta */
	private final Map<String, SchemaMeta> catalogs;
	private final Set<String> lockTables;
	private ReentrantLock metalock = new ReentrantLock();
	private Condition condRelease = metalock.newCondition();
	private ScheduledExecutorService scheduler;
	private ScheduledFuture<?> checkTaskHandler;
	public ProxyMetaManager() {
		this.catalogs = new ConcurrentHashMap<>();
		this.lockTables= new HashSet<>();
	}

	private String genLockKey(String schema, String tbName){
		return schema+"."+tbName;
	}
	public void addMetaLock(String schema, String tbName) throws InterruptedException {
		metalock.lock();
		try {
			String lockKey = genLockKey(schema, tbName);
			while(lockTables.contains(lockKey)){
				condRelease.await();
			}
			lockTables.add(lockKey);
		}finally {
			metalock.unlock();
		}
	}

	public void removeMetaLock(String schema, String tbName) {
		metalock.lock();
		try {
			lockTables.remove(genLockKey(schema, tbName));
			condRelease.signalAll();
		} finally {
			metalock.unlock();
		}
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
	 */
	public synchronized Map<String, SchemaMeta> getSchemas() {
		return catalogs;
	}

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
	 */
	private boolean checkDbExists(String schema) {
		return schema != null && this.catalogs.containsKey(schema);
	}

	private boolean checkTableExists(String schema, String strTable) {
		return checkDbExists(schema) && strTable != null && this.catalogs.get(schema).getTableMetas().containsKey(strTable);
	}

	public List<String> getTableNames(String schema) {
		List<String> tbNames;
		SchemaMeta schemaMeta = catalogs.get(schema);
		if (schemaMeta == null)
			return new ArrayList<>();
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

	public TableMeta getSyncTableMeta(String schema, String tbName) {
		while (true) {
			metalock.lock();
			try {
				if (lockTables.contains(genLockKey(schema, tbName))) {
					LOGGER.warn("schema:"+schema+", table:"+tbName+" is doing ddl,Waiting for table metadata lock");
					condRelease.await();
				} else {
					return getTableMeta(schema, tbName);
				}
			} catch (InterruptedException e) {
				return null;
			} finally {
				metalock.unlock();
			}
		}
	}

	private TableMeta getTableMeta(String schema, String tbName) {
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

	private Set<String> getSelfNodes(MycatConfig config) {
		Set<String> selfNode = null;
		for (Map.Entry<String, PhysicalDBPool> entry : config.getDataHosts().entrySet()) {
			PhysicalDBPool host = entry.getValue();
			DBHostConfig wHost = host.getSource().getConfig();
			if (("localhost".equalsIgnoreCase(wHost.getIp())||"127.0.0.1".equalsIgnoreCase(wHost.getIp())) && wHost.getPort() == config.getSystem().getServerPort()) {
				for (Map.Entry<String, PhysicalDBNode> nodeEntry : config.getDataNodes().entrySet()) {
					if (nodeEntry.getValue().getDbPool().getHostName().equals(host.getHostName())) {
						if (selfNode == null) {
							selfNode = new HashSet<>(2);
						}
						selfNode.add(nodeEntry.getKey());
					}
				}
				break;
			}
		}
		return selfNode;
	}
	public void init() throws Exception {
		if (MycatServer.getInstance().isUseZK()) {
			String ddlPath = ZKUtils.getZKBasePath() + ZookeeperPath.ZK_DDL.getKey();
			String lockPath = ZKUtils.getZKBasePath() + ZookeeperPath.ZK_LOCK .getKey()+ ZookeeperPath.ZK_SEPARATOR.getKey()
					+ ZookeeperPath.ZK_DDL.getKey() ;
			CuratorFramework zkConn = ZKUtils.getConnection();
			//CHECK DDL LOCK PATH HAS NOT CHILD
			while (zkConn.getChildren().forPath(lockPath).size() > 0) {
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
			}
			boolean createSuccess = false;
			while(!createSuccess) {
				try {
					//syncMeta LOCK ,if another server start, it may failed
					ZKUtils.createTempNode(lockPath, "syncMeta.lock");
					createSuccess= true;
				} catch (Exception e) {
					LOGGER.warn("createTempNode syncMeta.lock fialed",e);
					LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
				}
			}
			initMeta();
			// 创建online状态
			ZKUtils.createTempNode(ZKUtils.getZKBasePath()+ZookeeperPath.FLOW_ZK_PATH_ONLINE.getKey(), ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));

			ZKUtils.addChildPathCache(ddlPath, new DDLChildListener());
			// syncMeta UNLOCK
			zkConn.delete().forPath(lockPath + ZookeeperPath.ZK_SEPARATOR.getKey() + "syncMeta.lock");
		} else {
			initMeta();
		}
	}
	public void initMeta(){
		MycatConfig config = MycatServer.getInstance().getConfig();
		Set<String> selfNode = getSelfNodes(config);
		SchemaMetaHandler handler = new SchemaMetaHandler(config, selfNode);
		handler.execute();
		SystemConfig system = config.getSystem();
		if (system.getCheckTableConsistency() == 1) {
			scheduler= Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetaDataChecker-%d").build());
			checkTaskHandler = scheduler.scheduleWithFixedDelay(tableStructureCheckTask(selfNode), 0L, system.getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
		}
	}
	public void terminate(){
		if (checkTaskHandler != null) {
			checkTaskHandler.cancel(false);
			scheduler.shutdown();
		}
		catalogs.clear();
	}
	//定时检查不同分片表结构一致性
	private Runnable tableStructureCheckTask(final Set<String> selfNode) {
		return new Runnable() {
			@Override
			public void run() {
				tableStructureCheck(selfNode);
			}
		};
	}
	private void tableStructureCheck(Set<String> selfNode) {
		for (SchemaConfig schema : MycatServer.getInstance().getConfig().getSchemas().values()) {
			if (!checkDbExists(schema.getName())) {
				continue;
			}
			for (TableConfig table : schema.getTables().values()) {
				if (!checkTableExists(schema.getName(), table.getName())) {
					continue;
				}
				AbstractTableMetaHandler handler = new TableMetaCheckHandler(schema.getName(), table, selfNode);
				handler.execute();
			}
		}
	}
	public void updateMetaData(String schema, String sql, boolean isSuccess) {
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLStatement statement = parser.parseStatement();
		if (statement instanceof MySqlCreateTableStatement) {
			createTable(schema, sql, (MySqlCreateTableStatement) statement, isSuccess);
		} else if (statement instanceof SQLDropTableStatement) {
			dropTable(schema, sql, (SQLDropTableStatement) statement, isSuccess);
		} else if (statement instanceof SQLAlterTableStatement) {
			alterTable(schema, sql, (SQLAlterTableStatement) statement, isSuccess);
		} else if (statement instanceof SQLTruncateStatement) {
			// TODO:Sequence?
		} else if (statement instanceof SQLCreateIndexStatement) {
			createIndex(schema, sql, (SQLCreateIndexStatement) statement, isSuccess);
		} else if (statement instanceof SQLDropIndexStatement) {
			dropIndex(schema, sql, (SQLDropIndexStatement) statement, isSuccess);
		} else {
			// TODO: further
		}
	}

	public void notifyClusterDDL(String schema, String table, String sql, DDLStatus ddlStatus) throws Exception {
		CuratorFramework zkConn = ZKUtils.getConnection();
		DDLInfo ddlInfo = new DDLInfo(schema, sql, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), ddlStatus);
		String nodeName = StringUtil.getFullName(schema, table);
		String nodePath = ZKPaths.makePath(ZKUtils.getZKBasePath() + ZookeeperPath.ZK_DDL.getKey(), nodeName);
		if (zkConn.checkExists().forPath(nodePath) == null) {
			zkConn.create().forPath(nodePath, ddlInfo.toString().getBytes(StandardCharsets.UTF_8));
		} else {
			zkConn.setData().forPath(nodePath, ddlInfo.toString().getBytes(StandardCharsets.UTF_8));
			//zkLock， the other instance get the lock,than wait
			//TODO: IF SERVER OF DDL INSTANCE CRASH, MAY NEED REMOVE LOCK AND FRESH META MANUALLY
			boolean finished = false;
			String instancePath = ZKPaths.makePath(nodePath, ZookeeperPath.ZK_PATH_INSTANCE.getKey());
			zkConn.create().forPath(instancePath);
			InterProcessMutex distributeLock = new InterProcessMutex(zkConn, nodePath);
			distributeLock.acquire();
			try {
				ZKUtils.createTempNode(instancePath, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
				List<String> preparedList = zkConn.getChildren().forPath(instancePath);
				List<String> onlineList = zkConn.getChildren().forPath(ZKUtils.getZKBasePath() + FLOW_ZK_PATH_ONLINE.getKey());
				if(preparedList.size() == onlineList.size()) {
					finished =true;
				}
			} finally {
				distributeLock.release();
			}
			if (finished) {
				zkConn.delete().deletingChildrenIfNeeded().forPath(instancePath);
				zkConn.delete().deletingChildrenIfNeeded().forPath(nodePath);
			}
		}
	}

	//no need to check user
	private static SchemaInfo getSchemaInfo(String schema, SQLExprTableSource tableSource){
		try {
			return SchemaUtil.getSchemaInfo(null, schema, tableSource);
		}catch(SQLException e){// is should not happen
			LOGGER.warn("getSchemaInfo error",e);
			return null;
		}
	}
	private void createTable(String schema, String sql, MySqlCreateTableStatement statement, boolean isSuccess) {
		SchemaInfo schemaInfo = getSchemaInfo(schema, statement.getTableSource());
		try {
			if(!isSuccess){
				return;
			}
			TableMeta tblMeta = MetaHelper.initTableMeta(schemaInfo.table, statement, System.currentTimeMillis());
			addTable(schemaInfo.schema, tblMeta);
		} catch (Exception e) {
			LOGGER.warn("updateMetaData failed,sql is" + statement.toString(), e);
		} finally {
			removeMetaLock(schema, schemaInfo.table);
			if (MycatServer.getInstance().isUseZK()) {
				try {
					notifyClusterDDL(schemaInfo.schema, schemaInfo.table, sql, isSuccess ? DDLStatus.SUCCESS : DDLStatus.FAILED);
				} catch (Exception e) {
					LOGGER.warn("notifyClusterDDL error", e);
				}
			}
		}
	}

	/**
	 * In fact, it only have single table
	 */
	private void dropTable(String schema, String sql, SQLDropTableStatement statement, boolean isSuccess) {
		for (SQLExprTableSource table : statement.getTableSources()) {
			SchemaInfo schemaInfo = getSchemaInfo(schema, table);
			try {
				if(!isSuccess){
					return;
				}
				dropTable(schemaInfo.schema, schemaInfo.table);
			} catch (Exception e) {
				LOGGER.warn("updateMetaData failed,sql is" + statement.toString(), e);
			} finally {
				removeMetaLock(schema, schemaInfo.table);

				if (MycatServer.getInstance().isUseZK()) {
					try {
						notifyClusterDDL(schemaInfo.schema, schemaInfo.table, sql, isSuccess ? DDLStatus.SUCCESS : DDLStatus.FAILED);
					} catch (Exception e) {
						LOGGER.warn("notifyClusterDDL error", e);
					}
				}
			}
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

	private void alterTable(String schema, String sql, SQLAlterTableStatement alterStatement, boolean isSuccess) {
		SchemaInfo schemaInfo = getSchemaInfo(schema, alterStatement.getTableSource());
		try{
			if(!isSuccess){
				return;
			}
			TableMeta orgTbMeta = getTableMeta(schemaInfo.schema, schemaInfo.table);
			if (orgTbMeta == null)
				return;
			TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
			List<ColumnMeta> cols = new ArrayList<>();
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
					String dropName = StringUtil.removeBackQuote(dropIndex.getIndexName().getSimpleName());
					dropIndex(tmBuilder, dropName);
				} else if (alterItem instanceof SQLAlterTableDropKey) {
					SQLAlterTableDropKey dropIndex = (SQLAlterTableDropKey) alterItem;
					String dropName = StringUtil.removeBackQuote(dropIndex.getKeyName().getSimpleName());
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
//			long offset = -1;
//			if (ast.getTableOptions() != null && ast.getTableOptions().getAutoIncrement() != null) {
//				Object obj = ast.getTableOptions().getAutoIncrement()
//						.evaluation(ExprEvaluationUtil.getEvaluationParameters());
//				if (obj instanceof Number) {
//					offset = ((Number) obj).longValue();
//				}
//				tmBuilder.setAiOffset(offset);
//			}
			if (autoColumnIndex != -1) {
				tmBuilder.setAiColPos(autoColumnIndex);
			}
			tmBuilder.setVersion(System.currentTimeMillis());
			TableMeta newTblMeta = tmBuilder.build();
			addTable(schema, newTblMeta);
		} catch (Exception e) {
			LOGGER.warn("updateMetaData alterTable failed,sql is" + alterStatement.toString(), e);
		} finally {
			removeMetaLock(schema, schemaInfo.table);
			if (MycatServer.getInstance().isUseZK()) {
				try {
					notifyClusterDDL(schemaInfo.schema, schemaInfo.table, sql, isSuccess ? DDLStatus.SUCCESS : DDLStatus.FAILED);
				} catch (Exception e) {
					LOGGER.warn("notifyClusterDDL error", e);
				}
			}
		}
	}
	private void createIndex(String schema, String sql, SQLCreateIndexStatement statement, boolean isSuccess){
		SQLTableSource tableSource = statement.getTable();
		if (tableSource instanceof SQLExprTableSource) {
			SQLExprTableSource exprTableSource = (SQLExprTableSource) tableSource;
			SchemaInfo schemaInfo = getSchemaInfo(schema, exprTableSource);
			try{
				if(!isSuccess){
					return;
				}
				TableMeta orgTbMeta = getTableMeta(schemaInfo.schema, schemaInfo.table);
				if (orgTbMeta == null)
					return;
				String indexName = StringUtil.removeBackQuote(statement.getName().getSimpleName());
				TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
				if (statement.getType() == null) {
					addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.MUL, itemsToColumns(statement.getItems()));
				} else if (statement.getType().equals("UNIQUE")) {
					addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.UNI, itemsToColumns(statement.getItems()));
				}
			} catch (Exception e) {
				LOGGER.warn("updateMetaData failed,sql is" + statement.toString(), e);
			} finally {
				removeMetaLock(schema, schemaInfo.table);
				if (MycatServer.getInstance().isUseZK()) {
					try {
						notifyClusterDDL(schemaInfo.schema, schemaInfo.table, sql, isSuccess ? DDLStatus.SUCCESS : DDLStatus.FAILED);
					} catch (Exception e) {
						LOGGER.warn("notifyClusterDDL error", e);
					}
				}
			}
		}
	}
	private void addIndex(TableMeta.Builder tmBuilder, SQLAlterTableAddIndex addIndex) {
		List<SQLExpr> columnExprs = itemsToColumns(addIndex.getItems());
		Set<String> indexNames = new HashSet<>();
		if (addIndex.getName() == null) {
			for (IndexMeta index : tmBuilder.getIndexList()) {
				indexNames.add(index.getName());
			}
		}
		String indexName = MetaHelper.genIndexName(addIndex.getName(), columnExprs, indexNames);
		if (addIndex.isUnique()) {
			addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.UNI, columnExprs);
		} else {
			addIndex(indexName, tmBuilder, MetaHelper.INDEX_TYPE.MUL, columnExprs);
		}
	}
	private List<SQLExpr> itemsToColumns(List<SQLSelectOrderByItem> items){
		List<SQLExpr> columnExprs = new ArrayList<>();
		for (SQLSelectOrderByItem item :items) {
			columnExprs.add(item.getExpr());
		}
		return columnExprs;
	}
	private void addIndex(String indexName, TableMeta.Builder tmBuilder, INDEX_TYPE indexType, List<SQLExpr> columnExprs){

		IndexMeta indexMeta = MetaHelper.makeIndexMeta(indexName, indexType, columnExprs);
		tmBuilder.addIndex(indexMeta);
	}
	private void dropIndex(String schema, String sql, SQLDropIndexStatement dropIndexStatement, boolean isSuccess){
		SchemaInfo schemaInfo = getSchemaInfo(schema, dropIndexStatement.getTableName());
		TableMeta orgTbMeta = getTableMeta(schemaInfo.schema, schemaInfo.table);
		try {
			if(!isSuccess){
				return;
			}
			if (orgTbMeta != null) {
				TableMeta.Builder tmBuilder = orgTbMeta.toBuilder();
				String dropName = StringUtil.removeBackQuote(((SQLIdentifierExpr) dropIndexStatement.getIndexName()).getName());
				dropIndex(tmBuilder, dropName);
			}
		} catch (Exception e) {
			LOGGER.warn("updateMetaData failed,sql is" + dropIndexStatement.toString(), e);
		} finally {
			removeMetaLock(schema, schemaInfo.table);
			if (MycatServer.getInstance().isUseZK()) {
				try {
					notifyClusterDDL(schemaInfo.schema, schemaInfo.table, sql, isSuccess ? DDLStatus.SUCCESS : DDLStatus.FAILED);
				} catch (Exception e) {
					LOGGER.warn("notifyClusterDDL error", e);
				}
			}
		}
	}
	private void dropIndex(TableMeta.Builder tmBuilder, String dropName) {
		List<IndexMeta> indexs = new ArrayList<>();
		indexs.addAll(tmBuilder.getIndexList());
		if (dropIndex(indexs, dropName)) {
			tmBuilder.clearIndex().addAllIndex(indexs);
		} else {
			List<IndexMeta> uniques = new ArrayList<>();
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
				String afterColName = StringUtil.removeBackQuote(afterColumn.getSimpleName());
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
		String changeColName = StringUtil.removeBackQuote(changeColumn.getColumnName().getSimpleName());
		for (int i = 0; i < columnMetas.size(); i++) {
			String colName = columnMetas.get(i).getName();
			if (changeColName.equalsIgnoreCase(colName)) {
				columnMetas.remove(i);
				break;
			}
		}
		boolean isFirst = changeColumn.isFirst();
		SQLExpr afterColumn = changeColumn.getAfterColumn();
		int changeIndex = getChangeIndex(isFirst, afterColumn, columnMetas);
		ColumnMeta.Builder cmBuilder = MetaHelper.makeColumnMeta(changeColumn.getNewColumnDefinition());
		columnMetas.add(changeIndex, cmBuilder.build());
		if (cmBuilder.getAutoIncre()) {
			autoColumnIndex = changeIndex;
		}
		return autoColumnIndex;
	}

	private void dropColumn(List<ColumnMeta> columnMetas, SQLAlterTableDropColumnItem dropColumn) {
		for (SQLName dropName : dropColumn.getColumns()) {
			String dropColName = StringUtil.removeBackQuote(dropName.getSimpleName());
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
		String modifyColName = StringUtil.removeBackQuote(modifyColDef.getName().getSimpleName());
		for (int i = 0; i < columnMetas.size(); i++) {
			String colName = columnMetas.get(i).getName();
			if (modifyColName.equalsIgnoreCase(colName)) {
				columnMetas.remove(i);
				break;
			}
		}
		boolean isFirst = modifyColumn.isFirst();
		SQLExpr afterColumn = modifyColumn.getAfterColumn();
		int modifyIndex = getChangeIndex(isFirst, afterColumn, columnMetas);
		ColumnMeta.Builder cmBuilder = MetaHelper.makeColumnMeta(modifyColDef);
		columnMetas.add(modifyIndex, cmBuilder.build());
		if (cmBuilder.getAutoIncre()) {
			autoColumnIndex = modifyIndex;
		}
		return autoColumnIndex;
	}

	private int getChangeIndex(boolean isFirst, SQLExpr afterColumn, List<ColumnMeta> columnMetas ){
		int changeIndex = -1;
		if (isFirst) {
			changeIndex = 0;
		} else if (afterColumn != null) {
			String afterColName = StringUtil.removeBackQuote(((SQLIdentifierExpr) afterColumn).getName());
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
		return changeIndex;
	}

}
