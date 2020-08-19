/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.view.FileSystemRepository;
import com.actiontech.dble.backend.mysql.view.KVStoreRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.DDLChildListener;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.DbGroupResponseListener;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.DbGroupStatusListener;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.ViewChildListener;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.meta.table.AbstractSchemaMetaHandler;
import com.actiontech.dble.meta.table.DDLNotifyTableMetaHandler;
import com.actiontech.dble.meta.table.SchemaCheckMetaHandler;
import com.actiontech.dble.meta.table.ServerMetaHandler;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.ZKUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class ProxyMetaManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ProxyMetaManager.class);
    // catalog,table,table meta
    private final Map<String, SchemaMeta> catalogs;
    private final Map<String, String> lockTables;
    private ReentrantLock metaLock = new ReentrantLock();
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> checkTaskHandler;
    private AtomicInteger metaCount = new AtomicInteger(0);
    private volatile Repository repository = null;
    private AtomicInteger version = new AtomicInteger(0);
    private long timestamp;
    private AtomicInteger tableIndex = new AtomicInteger();

    public ProxyMetaManager() {
        this.catalogs = new ConcurrentHashMap<>();
        this.lockTables = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public ProxyMetaManager(ProxyMetaManager origin) {
        this.catalogs = new ConcurrentHashMap<>();
        this.lockTables = origin.lockTables;
        this.timestamp = origin.timestamp;
        this.metaLock = origin.metaLock;
        this.scheduler = origin.scheduler;
        this.metaCount = origin.metaCount;
        this.repository = origin.repository;
        this.version = origin.version;
        this.tableIndex = origin.tableIndex;
        for (Map.Entry<String, SchemaMeta> entry : origin.catalogs.entrySet()) {
            catalogs.put(entry.getKey(), entry.getValue().metaCopy());
        }
    }

    //no need to check user
    private static SchemaInfo getSchemaInfo(String schema, String table) {
        return SchemaUtil.getSchemaInfoWithoutCheck(schema, table);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getLockTables() {
        return lockTables;
    }

    private String genLockKey(String schema, String tbName) {
        return schema + "." + tbName;
    }

    public String metaCountCheck() {
        StringBuilder result = new StringBuilder();
        metaLock.lock();
        try {
            if (metaCount.get() != 0) {
                result.append("There is other session is doing DDL\n");
                for (String x : lockTables.values()) {
                    result.append(x).append("\n");
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

    private boolean isOnMetaLock(String schema, String tbName) {
        metaLock.lock();
        try {
            String lockKey = genLockKey(schema, tbName);
            return lockTables.containsKey(lockKey);
        } finally {
            metaLock.unlock();
        }
    }

    public boolean removeMetaLock(String schema, String tbName) {
        metaLock.lock();
        boolean isRemoved = false;
        try {
            if (lockTables.remove(genLockKey(schema, tbName)) != null) {
                isRemoved = true;
                metaCount.decrementAndGet();
            }
        } finally {
            metaLock.unlock();
        }
        return isRemoved;
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

    public void addTable(String schema, TableMeta tm) {
        String tbName = tm.getTableName();
        tm.setSchemaName(schema);
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null) {
            tm.setId(tableIndex.incrementAndGet());
            schemaMeta.addTableMeta(tbName, tm);
        }
    }

    public void addView(String schema, ViewMeta vm) {
        String viewName = vm.getViewName();
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null) {
            schemaMeta.addViewMeta(viewName, vm);
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
    public boolean dropTable(String schema, String table, String sql, boolean isSuccess, boolean needNotifyOther) {
        if (isSuccess) {
            dropTable(schema, table);
        }
        if (needNotifyOther) {
            try {
                notifyResponseClusterDDL(schema, table, sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.DROP_TABLE);
            } catch (Exception e) {
                LOGGER.warn("notifyResponseClusterDDL error", e);
            }
        }
        removeMetaLock(schema, table);
        return true;
    }

    public TableMeta getSyncTableMeta(String schema, String tbName) throws SQLNonTransientException {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-sync-meta");
        TraceManager.log(ImmutableMap.of("schema", schema, "table", tbName), traceObject);
        try {
            while (true) {
                int oldVersion = version.get();
                if (metaCount.get() == 0) {
                    TableMeta meta = getTableMeta(schema, tbName);
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
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public PlanNode getSyncView(String schema, String vName) throws SQLNonTransientException {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-sync-view");
        TraceManager.log(ImmutableMap.of("schema", schema, "table", vName), traceObject);
        try {
            while (true) {
                int oldVersion = version.get();
                if (metaCount.get() == 0) {
                    PlanNode viewNode = catalogs.get(schema).getView(vName);
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
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private TableMeta getTableMeta(String schema, String tbName) {
        return catalogs.get(schema).getTableMeta(tbName);
    }

    private Set<String> getSelfNodes(ServerConfig config) {
        Set<String> selfNode = null;
        for (Map.Entry<String, PhysicalDbGroup> entry : config.getDbGroups().entrySet()) {
            PhysicalDbGroup host = entry.getValue();
            if (host.isAllFakeNode()) {
                for (Map.Entry<String, ShardingNode> nodeEntry : config.getShardingNodes().entrySet()) {
                    if (nodeEntry.getValue().getDbGroup().getGroupName().equals(host.getGroupName())) {
                        if (selfNode == null) {
                            selfNode = new HashSet<>(2);
                        }
                        selfNode.add(nodeEntry.getKey());
                    }
                }
            }
        }
        return selfNode;
    }

    public void updateOnetableWithBackData(ServerConfig config, String schema, String tableName) {
        Set<String> selfNode = getSelfNodes(config);
        List<String> shardingNodes;
        if (config.getSchemas().get(schema).getTables().get(tableName) == null) {
            shardingNodes = Collections.singletonList(config.getSchemas().get(schema).getShardingNode());
        } else {
            shardingNodes = config.getSchemas().get(schema).getTables().get(tableName).getShardingNodes();
        }
        DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schema, tableName, shardingNodes, selfNode);
        handler.execute();
        removeMetaLock(schema, tableName);
    }

    public void init(ServerConfig config) throws Exception {
        LOGGER.info("init metaData start");
        tryAddSyncMetaLock();
        initMeta(config);
        releaseSyncMetaLock();
        LOGGER.info("init metaData end");
    }

    private void tryAddSyncMetaLock() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            int times = 0;
            DistributeLock lock = ClusterHelper.createDistributeLock(ClusterPathUtil.getSyncMetaLockPath(), String.valueOf(System.currentTimeMillis()));
            while (!lock.acquire()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                if (times % 100 == 0) {
                    LOGGER.warn("tryAddSyncMetaLock failed");
                    times = 0;
                }
                times++;
            }
            DistributeLockManager.addLock(lock);


            times = 0;
            String ddlLockPath = ClusterPathUtil.getDDLLockPath();
            while (!StringUtil.isEmpty(ClusterHelper.getPathKey(ddlLockPath)) && ClusterHelper.getChildrenSize(ddlLockPath) > 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                if (times % 60 == 0) {
                    LOGGER.warn("waiting for DDL finished ");
                    times = 0;
                }
                times++;
            }

            times = 0;
            String viewLockPath = ClusterPathUtil.getViewLockPath();
            while (!StringUtil.isEmpty(ClusterHelper.getPathKey(viewLockPath)) && ClusterHelper.getChildrenSize(viewLockPath) > 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                if (times % 60 == 0) {
                    LOGGER.warn("waiting for view finished");
                    times = 0;
                }
                times++;
            }
        }
    }

    private void releaseSyncMetaLock() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            if (ClusterConfig.getInstance().useZkMode()) {
                //add watcher
                ZKUtils.addChildPathCache(ClusterPathUtil.getDDLPath(), new DDLChildListener());
                //add tow ha status && ha lock watcher
                if (ClusterConfig.getInstance().isNeedSyncHa()) {
                    ZKUtils.addChildPathCache(ClusterPathUtil.getHaStatusPath(), new DbGroupStatusListener());
                    ZKUtils.addChildPathCache(ClusterPathUtil.getHaResponsePath(), new DbGroupResponseListener());
                }
                //add watcher
                ZKUtils.addChildPathCache(ClusterPathUtil.getViewChangePath(), new ViewChildListener());
            } else {
                ClusterToXml.startMetaListener();
            }
            // syncMeta UNLOCK
            DistributeLockManager.releaseLock(ClusterPathUtil.getSyncMetaLockPath());
        }
    }

    private void initViewMeta() {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            loadViewFromKV();
        } else {
            loadViewFromFile();
        }
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
     * put the create sql in & parse all the tableNode & put into sharding-viewMeta
     *
     * @param viewCreateSqlMap
     */
    public void loadViewMeta(Map<String, Map<String, String>> viewCreateSqlMap) {
        for (Map.Entry<String, Map<String, String>> schemaName : viewCreateSqlMap.entrySet()) {
            for (Map.Entry<String, String> view : schemaName.getValue().entrySet()) {
                try {
                    ViewMeta vm = new ViewMeta(schemaName.getKey(), view.getValue(), this);
                    vm.init();
                    SchemaMeta schemaMeta = this.getCatalogs().get(schemaName.getKey());
                    if (schemaMeta == null) {
                        LOGGER.warn("View " + view.getKey() + " can not find it's schema,view " + view.getKey() + " not initialized");
                    } else {
                        schemaMeta.getViewMetas().put(vm.getViewName(), vm);
                    }
                } catch (Exception e) {
                    LOGGER.warn("load view meta error", e);
                }
            }
        }
    }

    public void reloadViewMeta(Map<String, Map<String, String>> viewCreateSqlMap) {
        for (Map.Entry<String, Map<String, String>> schemaName : viewCreateSqlMap.entrySet()) {
            ConcurrentMap<String, ViewMeta> schemaViewMeta = new ConcurrentHashMap<String, ViewMeta>();
            for (Map.Entry<String, String> view : schemaName.getValue().entrySet()) {
                try {
                    ViewMeta vm = new ViewMeta(schemaName.getKey(), view.getValue(), this);
                    vm.init();
                    schemaViewMeta.put(vm.getViewName(), vm);
                } catch (Exception e) {
                    LOGGER.warn("reload view meta error", e);
                }
            }
            this.getCatalogs().get(schemaName.getKey()).setViewMetas(schemaViewMeta);
        }
    }

    /**
     * init meta when dble server started
     * no interrupted ,init the view anyway
     *
     * @param config
     */
    public void initMeta(ServerConfig config) {
        Set<String> selfNode = getSelfNodes(config);
        ServerMetaHandler handler = new ServerMetaHandler(this, config, selfNode);
        handler.setFilter(null);
        handler.execute();
        initViewMeta();
        if (SystemConfig.getInstance().getCheckTableConsistency() == 1) {
            scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetaDataChecker-%d").build());
            checkTaskHandler = scheduler.scheduleWithFixedDelay(tableStructureCheckTask(selfNode), SystemConfig.getInstance().getCheckTableConsistencyPeriod(), SystemConfig.getInstance().getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * init new Meta from reload config/metadata
     * can be interrupted and abandon the new config
     *
     * @param config
     * @param specifiedSchemas
     */
    public boolean initMeta(ServerConfig config, Map<String, Set<String>> specifiedSchemas) {
        Set<String> selfNode = getSelfNodes(config);
        ServerMetaHandler handler = new ServerMetaHandler(this, config, selfNode);
        handler.setFilter(specifiedSchemas);
        handler.register();
        // if the meta reload interrupted by reload release
        // do not reload the view meta or start a new scheduler
        if (handler.execute()) {
            initViewMeta();
            if (SystemConfig.getInstance().getCheckTableConsistency() == 1) {
                scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetaDataChecker-%d").build());
                checkTaskHandler = scheduler.scheduleWithFixedDelay(tableStructureCheckTask(selfNode), SystemConfig.getInstance().getCheckTableConsistencyPeriod(), SystemConfig.getInstance().getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
            }
            return true;
        }
        return false;
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
            Map<String, Set<String>> shardingNodeMap = new HashMap<>();
            for (Map.Entry<String, BaseTableConfig> entry : schema.getTables().entrySet()) {
                String tableName = entry.getKey();
                BaseTableConfig tbConfig = entry.getValue();
                for (String shardingNode : tbConfig.getShardingNodes()) {
                    Set<String> tables = shardingNodeMap.computeIfAbsent(shardingNode, k -> new HashSet<>());
                    tables.add(tableName);
                }
            }

            AbstractSchemaMetaHandler multiTablesMetaHandler = new SchemaCheckMetaHandler(this, schema, selfNode);
            multiTablesMetaHandler.execute();
        }
    }

    public boolean updateMetaData(String schema, String tableName, String sql, boolean isSuccess, DDLInfo.DDLType ddlType) {
        if (ddlType == DDLInfo.DDLType.DROP_TABLE) {
            return dropTable(schema, tableName, sql, isSuccess, true);
        } else if (ddlType == DDLInfo.DDLType.TRUNCATE_TABLE) {
            return truncateTable(schema, tableName, sql, isSuccess);
        } else if (ddlType == DDLInfo.DDLType.CREATE_TABLE) {
            return createTable(schema, tableName, sql, isSuccess);
        } else {
            return generalDDL(schema, tableName, sql, isSuccess);
        }
    }

    public void notifyClusterDDL(String schema, String table, String sql) throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            if (DistributeLockManager.isLooked(ClusterPathUtil.getSyncMetaLockPath())) {
                String msg = "There is another instance init meta data, try it later.";
                throw new Exception(msg);
            }
            DDLInfo ddlInfo = new DDLInfo(schema, sql, SystemConfig.getInstance().getInstanceName(), DDLInfo.DDLStatus.INIT, DDLInfo.DDLType.UNKNOWN);
            String tableFullName = StringUtil.getUFullName(schema, table);
            String tableDDLPath = ClusterPathUtil.getDDLPath(tableFullName);
            String ddlLockPath = ClusterPathUtil.getDDLLockPath(tableFullName);
            DistributeLock lock = ClusterHelper.createDistributeLock(ddlLockPath, ddlInfo.toString());
            if (!lock.acquire()) {
                String msg = "The metaLock about `" + tableFullName + "` is exists. It means other instance is doing DDL.";
                LOGGER.info(msg + " The path of DDL is " + tableDDLPath);
                throw new Exception(msg);
            }
            DistributeLockManager.addLock(lock);
            ClusterDelayProvider.delayAfterDdlLockMeta();
            ClusterHelper.setKV(tableDDLPath, ddlInfo.toString());
        }
    }

    public void notifyResponseClusterDDL(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType) throws Exception {
        ClusterDelayProvider.delayAfterDdlExecuted();
        if (ClusterConfig.getInstance().isClusterEnable()) {
            String tableFullName = StringUtil.getUFullName(schema, table);
            String tableDDLPath = ClusterPathUtil.getDDLPath(tableFullName);
            metaLock.lock();
            boolean isLock = true;
            try {
                if (isOnMetaLock(schema, table)) {
                    ClusterDelayProvider.delayBeforeDdlNotice();
                    DDLInfo ddlInfo = new DDLInfo(schema, sql, SystemConfig.getInstance().getInstanceName(), ddlStatus, ddlType);
                    ClusterHelper.setKV(tableDDLPath, ddlInfo.toString());
                    ClusterDelayProvider.delayAfterDdlNotice();
                    ClusterHelper.createSelfTempNode(tableDDLPath, ClusterPathUtil.SUCCESS);
                    metaLock.unlock();
                    isLock = false;
                    String errorMsg = ClusterLogic.waitingForAllTheNode(tableDDLPath, ClusterPathUtil.SUCCESS);
                    if (errorMsg != null) {
                        throw new RuntimeException(errorMsg);
                    }
                } else {
                    metaLock.unlock();
                    isLock = false;
                }
            } finally {
                if (isLock) {
                    metaLock.unlock();
                }
                ClusterDelayProvider.delayBeforeDdlNoticeDeleted();
                ClusterHelper.cleanPath(tableDDLPath);
                //release the lock
                ClusterDelayProvider.delayBeforeDdlLockRelease();
                DistributeLockManager.releaseLock(ClusterPathUtil.getDDLLockPath(tableFullName));
            }
        }
    }


    private boolean createTable(String schema, String table, String sql, boolean isSuccess) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, table);
        boolean result = isSuccess;
        if (isSuccess) {
            BaseTableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(table);
            String showShardingNode = schemaInfo.getSchemaConfig().getShardingNode();
            if (tbConfig != null) {
                for (String shardingNode : tbConfig.getShardingNodes()) {
                    showShardingNode = shardingNode;
                    String tableId = "sharding_node[" + shardingNode + "]:Table[" + table + "]";
                    if (ToResolveContainer.TABLE_LACK.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId), ToResolveContainer.TABLE_LACK, tableId);
                    }
                }
            }
            DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schema, table, Collections.singletonList(showShardingNode), null);
            handler.execute();
            result = handler.isMetaInited();
        }
        try {
            notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.CREATE_TABLE);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        return result;
    }


    private boolean truncateTable(String schema, String table, String sql, boolean isSuccess) {
        try {
            notifyResponseClusterDDL(schema, table, sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.TRUNCATE_TABLE);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schema, table);
        return true;
    }


    private boolean generalDDL(String schema, String table, String sql, boolean isSuccess) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, table);
        boolean result = isSuccess;
        if (isSuccess) {
            result = genTableMetaByShow(schemaInfo);
        }
        try {
            notifyResponseClusterDDL(schemaInfo.getSchema(), schemaInfo.getTable(), sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.DROP_INDEX);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schemaInfo.getSchema(), schemaInfo.getTable());
        return result;
    }

    private boolean genTableMetaByShow(SchemaInfo schemaInfo) {
        String tableName = schemaInfo.getTable();
        BaseTableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(tableName);
        String showShardingNode = schemaInfo.getSchemaConfig().getShardingNode();
        if (tbConfig != null) {
            for (String shardingNode : tbConfig.getShardingNodes()) {
                showShardingNode = shardingNode;
                break;
            }
        }
        DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schemaInfo.getSchema(), tableName, Collections.singletonList(showShardingNode), null);
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
