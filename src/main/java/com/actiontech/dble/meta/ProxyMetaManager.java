/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.mysql.view.CKVStoreRepository;
import com.actiontech.dble.backend.mysql.view.FileSystemRepository;
import com.actiontech.dble.backend.mysql.view.KVStoreRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.model.DBHostConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.meta.table.AbstractTablesMetaHandler;
import com.actiontech.dble.meta.table.DDLNotifyTableMetaHandler;
import com.actiontech.dble.meta.table.SchemaMetaHandler;
import com.actiontech.dble.meta.table.TablesMetaCheckHandler;
import com.actiontech.dble.meta.table.old.AbstractTableMetaHandler;
import com.actiontech.dble.meta.table.old.TableMetaCheckHandler;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.ZKUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class ProxyMetaManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ProxyMetaManager.class);
    /* catalog,table,tablemeta */
    private final Map<String, SchemaMeta> catalogs;
    private final Map<String, String> lockTables;
    private ReentrantLock metaLock = new ReentrantLock();
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> checkTaskHandler;
    private AtomicInteger metaCount = new AtomicInteger(0);
    private volatile Repository repository = null;
    private AtomicInteger version = new AtomicInteger(0);
    private final long timestamp;

    public ProxyMetaManager() {
        this.catalogs = new ConcurrentHashMap<>();
        this.lockTables = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public long getTimestamp() {
        return timestamp;
    }

    private String genLockKey(String schema, String tbName) {
        return schema + "." + tbName;
    }

    public String metaCountCheck() {
        StringBuffer result = new StringBuffer("");
        metaLock.lock();
        try {
            if (metaCount.get() != 0) {
                result.append("There is other session is doing DDL\n");
                for (String x : lockTables.values()) {
                    result.append(x + "\n");
                }
                result.setLength(result.length() - 1);
            }
        } finally {
            metaLock.unlock();
        }
        return result.length() == 0 ? null : result.toString();
    }

    public ReentrantLock getMetaLock() {
        return metaLock;
    }

    public void addMetaLock(String schema, String tbName, String sql) throws SQLNonTransientException {
        metaLock.lock();
        try {
            String lockKey = genLockKey(schema, tbName);
            if (lockTables.containsKey(lockKey)) {
                String msg = "SCHEMA[" + schema + "], TABLE[" + tbName + "] is doing DDL";
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg, "HY000", ErrorCode.ER_DOING_DDL);
            } else {
                metaCount.incrementAndGet();
                version.incrementAndGet();
                lockTables.put(lockKey, sql);
            }
        } finally {
            metaLock.unlock();
        }
    }

    public void removeMetaLock(String schema, String tbName) {
        metaLock.lock();
        try {
            if (lockTables.remove(genLockKey(schema, tbName)) != null) {
                metaCount.decrementAndGet();
            }
        } finally {
            metaLock.unlock();
        }
    }

    public Map<String, SchemaMeta> getCatalogs() {
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
     */
    private boolean checkDbExists(String schema) {
        return schema != null && this.catalogs.containsKey(schema);
    }

    public boolean checkTableExists(String schema, String strTable) {
        return checkDbExists(schema) && strTable != null && this.catalogs.get(schema).getTableMetas().containsKey(strTable);
    }

    public void addTable(String schema, StructureMeta.TableMeta tm) {
        String tbName = tm.getTableName();
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null) {
            schemaMeta.addTableMeta(tbName, tm);
        }
    }


    private void dropTable(String schema, String tbName) {
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null)
            schemaMeta.dropTable(tbName);
    }

    /**
     * In fact, it only have single table
     */
    private boolean dropTable(String schema, String sql, SQLDropTableStatement statement, boolean isSuccess, boolean needNotifyOther) {
        for (SQLExprTableSource table : statement.getTableSources()) {
            SchemaInfo schemaInfo = getSchemaInfo(schema, table);
            if (isSuccess) {
                dropTable(schemaInfo.getSchema(), schemaInfo.getTable());
            }
            try {
                notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.DROP_TABLE, needNotifyOther);
            } catch (Exception e) {
                LOGGER.warn("notifyResponseClusterDDL error", e);
            }
            removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        }
        return true;
    }

    public StructureMeta.TableMeta getSyncTableMeta(String schema, String tbName) throws SQLNonTransientException {
        while (true) {
            int oldVersion = version.get();
            if (metaCount.get() == 0) {
                StructureMeta.TableMeta meta = getTableMeta(schema, tbName);
                if (version.get() == oldVersion) {
                    return meta;
                }
            } else {
                metaLock.lock();
                try {
                    if (lockTables.containsKey(genLockKey(schema, tbName))) {
                        String msg = "SCHEMA[" + schema + "], TABLE[" + tbName + "] is doing DDL";
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg, "HY000", ErrorCode.ER_DOING_DDL);
                    } else {
                        return getTableMeta(schema, tbName);
                    }
                } finally {
                    metaLock.unlock();
                }
            }
        }
    }


    public QueryNode getSyncView(String schema, String vName) throws SQLNonTransientException {
        while (true) {
            int oldVersion = version.get();
            if (metaCount.get() == 0) {
                QueryNode viewNode = catalogs.get(schema).getView(vName);
                if (version.get() == oldVersion) {
                    return viewNode;
                }
            } else {
                metaLock.lock();
                try {
                    if (lockTables.containsKey(genLockKey(schema, vName))) {
                        String msg = "SCHEMA[" + schema + "], TABLE[" + vName + "] is doing DDL";
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg, "HY000", ErrorCode.ER_DOING_DDL);
                    } else {
                        return catalogs.get(schema).getView(vName);
                    }
                } finally {
                    metaLock.unlock();
                }
            }
        }
    }

    private StructureMeta.TableMeta getTableMeta(String schema, String tbName) {
        return catalogs.get(schema).getTableMeta(tbName);
    }


    private Set<String> getSelfNodes(ServerConfig config) {
        Set<String> selfNode = null;
        for (Map.Entry<String, PhysicalDBPool> entry : config.getDataHosts().entrySet()) {
            PhysicalDBPool host = entry.getValue();
            DBHostConfig wHost = host.getSource().getConfig();
            if (("localhost".equalsIgnoreCase(wHost.getIp()) || "127.0.0.1".equalsIgnoreCase(wHost.getIp())) && wHost.getPort() == config.getSystem().getServerPort()) {
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

    public void updateOnetableWithBackData(ServerConfig config, String schema, String tableName) {
        Set<String> selfNode = getSelfNodes(config);
        List<String> dataNodes;
        if (config.getSchemas().get(schema).getTables().get(tableName) == null) {
            dataNodes = Collections.singletonList(config.getSchemas().get(schema).getDataNode());
        } else {
            dataNodes = config.getSchemas().get(schema).getTables().get(tableName).getDataNodes();
        }
        DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schema, tableName, dataNodes, selfNode);
        handler.execute();
        removeMetaLock(schema, tableName);
    }


    public void init(ServerConfig config) throws Exception {
        LOGGER.info("init metaData start");
        if (DbleServer.getInstance().isUseZK()) {
            this.metaZKinit(config);
        } else {
            initMeta(config);
        }
        LOGGER.info("init metaData end");
    }

    private void metaZKinit(ServerConfig config) throws Exception {
        //add syncMeta.lock the other DDL will wait
        boolean createSuccess = false;
        int times = 0;
        while (!createSuccess) {
            try {
                //syncMeta LOCK ,if another server start, it may failed
                ZKUtils.createTempNode(KVPathUtil.getSyncMetaLockPath());
                createSuccess = true;
            } catch (Exception e) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                if (times % 60 == 0) {
                    LOGGER.info("createTempNode syncMeta.lock failed", e);
                    times = 0;
                }
                times++;
            }
        }
        String ddlPath = KVPathUtil.getDDLPath();
        CuratorFramework zkConn = ZKUtils.getConnection();
        //WAIT DDL PATH HAS NOT CHILD
        times = 0;
        while (zkConn.getChildren().forPath(ddlPath).size() > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
            if (times % 60 == 0) {
                LOGGER.info("waiting for DDL in " + ddlPath);
                times = 0;
            }
            times++;
        }

        initMeta(config);
        // online
        ZKUtils.createTempNode(KVPathUtil.getOnlinePath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        //add watcher
        ZKUtils.addChildPathCache(ddlPath, new DDLChildListener());
        //add watcher
        ZKUtils.addViewPathCache(KVPathUtil.getViewPath(), new ViewChildListener());
        // syncMeta UNLOCK
        zkConn.delete().forPath(KVPathUtil.getSyncMetaLockPath());
    }


    /**
     * recovery all the view info from KV system
     */
    private void loadViewFromKV() {
        repository = new KVStoreRepository();
        Map<String, Map<String, String>> viewCreateSqlMap = repository.getViewCreateSqlMap();
        loadViewMeta(viewCreateSqlMap);
    }

    /**
     * recovery all the view info for file system
     */
    private void loadViewFromFile() {
        repository = new FileSystemRepository();
        Map<String, Map<String, String>> viewCreateSqlMap = repository.getViewCreateSqlMap();
        loadViewMeta(viewCreateSqlMap);
    }

    /**
     * recovery all the view info from ckvsystem
     */
    private void loadViewFromCKV() {
        try {
            repository = new CKVStoreRepository();
        } catch (Exception e) {
            LOGGER.info("load view info from ucore fail,turned into local file model");
            repository = new FileSystemRepository();
        }
        Map<String, Map<String, String>> viewCreateSqlMap = repository.getViewCreateSqlMap();
        loadViewMeta(viewCreateSqlMap);
    }

    /**
     * put the create sql in & parse all the tableNode & put into schema-viewMeta
     *
     * @param viewCreateSqlMap
     */
    public void loadViewMeta(Map<String, Map<String, String>> viewCreateSqlMap) {
        for (Map.Entry<String, Map<String, String>> schemaName : viewCreateSqlMap.entrySet()) {
            for (Map.Entry<String, String> view : schemaName.getValue().entrySet()) {
                ViewMeta vm = new ViewMeta(view.getValue(), schemaName.getKey(), this);
                vm.init(true);
                this.getCatalogs().get(schemaName.getKey()).getViewMetas().put(vm.getViewName(), vm);
            }
        }
    }

    public void reloadViewMeta(Map<String, Map<String, String>> viewCreateSqlMap) {
        for (Map.Entry<String, Map<String, String>> schemaName : viewCreateSqlMap.entrySet()) {
            ConcurrentMap<String, ViewMeta> schemaViewMeta = new ConcurrentHashMap<String, ViewMeta>();
            for (Map.Entry<String, String> view : schemaName.getValue().entrySet()) {
                ViewMeta vm = new ViewMeta(view.getValue(), schemaName.getKey(), this);
                vm.init(true);
                schemaViewMeta.put(vm.getViewName(), vm);
            }
            this.getCatalogs().get(schemaName.getKey()).setViewMetas(schemaViewMeta);
        }
    }


    public void initMeta(ServerConfig config) {
        Set<String> selfNode = getSelfNodes(config);
        SchemaMetaHandler handler = new SchemaMetaHandler(this, config, selfNode);
        handler.execute();
        if (DbleServer.getInstance().isUseZK()) {
            loadViewFromKV();
        } else if (DbleServer.getInstance().isUseUcore()) {
            loadViewFromCKV();
        } else {
            loadViewFromFile();
        }
        SystemConfig system = config.getSystem();
        if (system.getCheckTableConsistency() == 1) {
            scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetaDataChecker-%d").build());
            checkTaskHandler = scheduler.scheduleWithFixedDelay(tableStructureCheckTask(selfNode), system.getCheckTableConsistencyPeriod(), system.getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
        }
    }

    public void terminate() {
        if (checkTaskHandler != null) {
            checkTaskHandler.cancel(false);
            scheduler.shutdown();
        }
        if (repository != null) {
            repository.terminate();
        }
    }

    //Check the Consistency of table Structure
    private Runnable tableStructureCheckTask(final Set<String> selfNode) {
        return new Runnable() {
            @Override
            public void run() {
                tableStructureCheck(selfNode);
            }
        };
    }

    private void tableStructureCheck(Set<String> selfNode) {
        for (SchemaConfig schema : DbleServer.getInstance().getConfig().getSchemas().values()) {
            if (!checkDbExists(schema.getName())) {
                continue;
            }
            if (DbleServer.getInstance().getConfig().getSystem().getUseOldMetaInit() == 1) {
                for (TableConfig table : schema.getTables().values()) {
                    if (!checkTableExists(schema.getName(), table.getName())) {
                        continue;
                    }
                    AbstractTableMetaHandler handler = new TableMetaCheckHandler(this, schema.getName(), table, selfNode);
                    handler.execute();
                }
            } else {
                Map<String, Set<String>> dataNodeMap = new HashMap<>();
                for (Map.Entry<String, TableConfig> entry : schema.getTables().entrySet()) {
                    String tableName = entry.getKey();
                    TableConfig tbConfig = entry.getValue();
                    for (String dataNode : tbConfig.getDataNodes()) {
                        Set<String> tables = dataNodeMap.get(dataNode);
                        if (tables == null) {
                            tables = new HashSet<>();
                            dataNodeMap.put(dataNode, tables);
                        }
                        tables.add(tableName);
                    }
                }

                AbstractTablesMetaHandler tableHandler = new TablesMetaCheckHandler(this, schema.getName(), dataNodeMap, selfNode);
                tableHandler.execute();
            }
        }
    }

    public boolean updateMetaData(String schema, String sql, boolean isSuccess, boolean needNotifyOther) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        if (statement instanceof SQLDropTableStatement) {
            return dropTable(schema, sql, (SQLDropTableStatement) statement, isSuccess, needNotifyOther);
        } else if (statement instanceof MySqlCreateTableStatement) {
            return createTable(schema, sql, (MySqlCreateTableStatement) statement, isSuccess, needNotifyOther);
        } else if (statement instanceof SQLAlterTableStatement) {
            return alterTable(schema, sql, (SQLAlterTableStatement) statement, isSuccess, needNotifyOther);
        } else if (statement instanceof SQLTruncateStatement) {
            return truncateTable(schema, sql, (SQLTruncateStatement) statement, isSuccess, needNotifyOther);
        } else if (statement instanceof SQLCreateIndexStatement) {
            return createIndex(schema, sql, (SQLCreateIndexStatement) statement, isSuccess, needNotifyOther);
        } else if (statement instanceof SQLDropIndexStatement) {
            return dropIndex(schema, sql, (SQLDropIndexStatement) statement, isSuccess, needNotifyOther);
        } else {
            return true;
            // TODO: further
        }

    }

    public void notifyClusterDDL(String schema, String table, String sql) throws Exception {
        if (DbleServer.getInstance().isUseZK()) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            DDLInfo ddlInfo = new DDLInfo(schema, sql, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), DDLInfo.DDLStatus.INIT, DDLInfo.DDLType.UNKNOWN);
            String nodeName = StringUtil.getFullName(schema, table);
            String nodePath = ZKPaths.makePath(KVPathUtil.getDDLPath(), nodeName);
            zkConn.create().forPath(nodePath, ddlInfo.toString().getBytes(StandardCharsets.UTF_8));
        } else if (DbleServer.getInstance().isUseUcore()) {
            DDLInfo ddlInfo = new DDLInfo(schema, sql, UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), DDLInfo.DDLStatus.INIT, DDLInfo.DDLType.UNKNOWN);
            String nodeName = StringUtil.getUFullName(schema, table);
            String ddlPath = UcorePathUtil.getDDLPath(nodeName);
            UDistributeLock lock = new UDistributeLock(ddlPath, ddlInfo.toString());
            //ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLPath(nodeName), ddlInfo.toString());
            if (!lock.acquire()) {
                String msg = "The syncMeta.lock or metaLock about " + nodeName + " in " + ddlPath + "is Exists";
                LOGGER.info(msg);
                throw new Exception(msg);
            }
            ClusterDelayProvider.delayAfterDdlLockMeta();
            UDistrbtLockManager.addLock(lock);
        }
    }


    public void notifyResponseClusterDDL(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType, boolean needNotifyOther) throws Exception {
        if (DbleServer.getInstance().isUseZK()) {
            notifyResponseZKDdl(schema, table, sql, ddlStatus, ddlType, needNotifyOther);
        } else if (DbleServer.getInstance().isUseUcore()) {
            ClusterDelayProvider.delayAfterDdlExecuted();
            notifyReponseUcoreDDL(schema, table, sql, ddlStatus, ddlType, needNotifyOther);
        }
    }

    private void notifyResponseZKDdl(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType, boolean needNotifyOther) throws Exception {
        CuratorFramework zkConn = ZKUtils.getConnection();
        DDLInfo ddlInfo = new DDLInfo(schema, sql, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ddlStatus, ddlType);
        String nodeName = StringUtil.getFullName(schema, table);
        String nodePath = ZKPaths.makePath(KVPathUtil.getDDLPath(), nodeName);

        String instancePath = ZKPaths.makePath(nodePath, KVPathUtil.DDL_INSTANCE);
        String thisNode = ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
        ZKUtils.createTempNode(instancePath, thisNode);
        if (needNotifyOther) {
            zkConn.setData().forPath(nodePath, ddlInfo.toString().getBytes(StandardCharsets.UTF_8));
            while (true) {
                List<String> preparedList = zkConn.getChildren().forPath(instancePath);
                List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                if (preparedList.size() >= onlineList.size()) {
                    zkConn.delete().deletingChildrenIfNeeded().forPath(nodePath);
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }

    /**
     * Notify the ucore Cluster to do things
     *
     * @param schema
     * @param table
     * @param sql
     * @param ddlStatus
     * @param needNotifyOther
     * @throws Exception
     */
    public void notifyReponseUcoreDDL(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType, boolean needNotifyOther) throws Exception {
        String nodeName = StringUtil.getUFullName(schema, table);
        DDLInfo ddlInfo = new DDLInfo(schema, sql, UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ddlStatus, ddlType);
        ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLInstancePath(nodeName), UcorePathUtil.SUCCESS);
        if (needNotifyOther) {
            try {
                ClusterDelayProvider.delayBeforeDdlNotice();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLPath(nodeName), ddlInfo.toString());
                ClusterDelayProvider.delayAfterDdlNotice();

                String errorMsg = ClusterUcoreSender.waitingForAllTheNode(UcorePathUtil.SUCCESS, UcorePathUtil.getDDLPath(nodeName));

                if (errorMsg != null) {
                    throw new RuntimeException(errorMsg);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                ClusterDelayProvider.delayBeforeDdlNoticeDeleted();
                ClusterUcoreSender.deleteKVTree(UcorePathUtil.getDDLPath(nodeName) + "/");
                //release the lock
                ClusterDelayProvider.delayBeforeDdlLockRelease();
                UDistrbtLockManager.releaseLock(UcorePathUtil.getDDLPath(nodeName));
            }
        }

    }


    //no need to check user
    private static SchemaInfo getSchemaInfo(String schema, SQLExprTableSource tableSource) {
        try {
            return SchemaUtil.getSchemaInfo(null, schema, tableSource);
        } catch (SQLException e) { // is should not happen
            LOGGER.info("getSchemaInfo error", e);
            return null;
        }
    }


    private boolean createTable(String schema, String sql, MySqlCreateTableStatement statement, boolean isSuccess, boolean needNotifyOther) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, statement.getTableSource());
        boolean result = isSuccess;
        if (isSuccess) {
            String tableName = schemaInfo.getTable();
            TableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(tableName);
            String showDataNode = schemaInfo.getSchemaConfig().getDataNode();
            if (tbConfig != null) {
                for (String dataNode : tbConfig.getDataNodes()) {
                    showDataNode = dataNode;
                    String tableId = "DataNode[" + dataNode + "]:Table[" + tableName + "]";
                    if (ToResolveContainer.TABLE_LACK.contains(tableId) && AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId))) {
                        ToResolveContainer.TABLE_LACK.remove(tableId);
                    }
                }
            }
            DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schemaInfo.getSchema(), tableName, Collections.singletonList(showDataNode), null);
            handler.execute();
            result = handler.isMetaInited();
        }
        try {
            notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.CREATE_TABLE, needNotifyOther);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        return result;
    }

    private boolean alterTable(String schema, String sql, SQLAlterTableStatement alterStatement, boolean isSuccess, boolean needNotifyOther) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, alterStatement.getTableSource());
        boolean result = isSuccess;
        if (isSuccess) {
            result = genTableMetaByShow(schemaInfo);
        }
        try {
            notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.ALTER_TABLE, needNotifyOther);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        return result;
    }

    private boolean truncateTable(String schema, String sql, SQLTruncateStatement statement, boolean isSuccess, boolean needNotifyOther) {
        //TODO:reset Sequence?
        SQLExprTableSource exprTableSource = statement.getTableSources().get(0);
        SchemaInfo schemaInfo = getSchemaInfo(schema, exprTableSource);
        try {
            notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.TRUNCATE_TABLE, needNotifyOther);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        return true;
    }

    private boolean createIndex(String schema, String sql, SQLCreateIndexStatement statement, boolean isSuccess, boolean needNotifyOther) {
        SQLTableSource tableSource = statement.getTable();
        if (tableSource instanceof SQLExprTableSource) {
            SQLExprTableSource exprTableSource = (SQLExprTableSource) tableSource;
            SchemaInfo schemaInfo = getSchemaInfo(schema, exprTableSource);
            boolean result = isSuccess;
            if (isSuccess) {
                result = genTableMetaByShow(schemaInfo);
            }
            try {
                notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.CREATE_INDEX, needNotifyOther);
            } catch (Exception e) {
                LOGGER.warn("notifyResponseClusterDDL error", e);
            }
            removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
            return result;
        }
        return true;
    }

    private boolean dropIndex(String schema, String sql, SQLDropIndexStatement dropIndexStatement, boolean isSuccess, boolean needNotifyOther) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, dropIndexStatement.getTableName());
        boolean result = isSuccess;
        if (isSuccess) {
            result = genTableMetaByShow(schemaInfo);
        }
        try {
            notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.DROP_INDEX, needNotifyOther);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        return result;
    }

    private boolean genTableMetaByShow(SchemaInfo schemaInfo) {
        String tableName = schemaInfo.getTable();
        TableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(tableName);
        String showDataNode = schemaInfo.getSchemaConfig().getDataNode();
        if (tbConfig != null) {
            for (String dataNode : tbConfig.getDataNodes()) {
                showDataNode = dataNode;
                break;
            }
        }
        DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schemaInfo.getSchema(), tableName, Collections.singletonList(showDataNode), null);
        handler.execute();
        return handler.isMetaInited();
    }

    public Repository getRepository() {
        return repository;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
