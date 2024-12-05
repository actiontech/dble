/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.view.FileSystemRepository;
import com.oceanbase.obsharding_d.backend.mysql.view.KVStoreRepository;
import com.oceanbase.obsharding_d.backend.mysql.view.Repository;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.DistributeLockManager;
import com.oceanbase.obsharding_d.cluster.general.kVtoXml.ClusterToXml;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterTime;
import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen.DDLChildListener;
import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen.DbGroupResponseListener;
import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen.DbGroupStatusListener;
import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen.ViewChildListener;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.meta.table.AbstractSchemaMetaHandler;
import com.oceanbase.obsharding_d.meta.table.SchemaCheckMetaHandler;
import com.oceanbase.obsharding_d.meta.table.ServerMetaHandler;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.singleton.TraceManager;
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
    private static AtomicInteger fakeTableIndex = new AtomicInteger();

    public ProxyMetaManager() {
        this.catalogs = new ConcurrentHashMap<>();
        this.lockTables = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public ProxyMetaManager(ProxyMetaManager origin) {
        this.catalogs = new ConcurrentHashMap<>();
        this.lockTables = origin.lockTables;
        this.timestamp = System.currentTimeMillis();
        this.metaLock = origin.metaLock;
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

    public boolean removeMetaLock(String schema, String tbName) {

        boolean isRemoved = false;
        metaLock.lock();
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

    public void addTable(String schema, TableMeta tm, boolean isNewCreate) {
        String tbName = tm.getTableName();
        tm.setSchemaName(schema);
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null) {
            tm.setId(tableIndex.incrementAndGet());
            schemaMeta.addTableMeta(tbName, tm);
        }
        if (isNewCreate) {
            SchemaUtil.tryAddDefaultShardingTableConfig(schema, tbName, tm.getCreateSql(), fakeTableIndex);
        }
    }

    public void addView(String schema, ViewMeta vm) {
        String viewName = vm.getViewName();
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null) {
            schemaMeta.addViewMeta(viewName, vm);
        }
    }

    public void dropTable(String schema, String tbName) {
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null)
            schemaMeta.dropTable(tbName);
        SchemaUtil.tryDropDefaultShardingTableConfig(schema, tbName);
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
                if (catalogs.get(schema) == null) {
                    return null;
                }
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

    protected static Set<String> getSelfNodes(ServerConfig config) {
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
            final ClusterHelper metaHelper = ClusterHelper.getInstance(ClusterOperation.META);
            DistributeLock lock = metaHelper.createDistributeLock(ClusterMetaUtil.getSyncMetaLockPath(), new ClusterTime(String.valueOf(System.currentTimeMillis())));
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
            final ClusterHelper ddlHelper = ClusterHelper.getInstance(ClusterOperation.DDL);
            while (!Boolean.TRUE.equals(ClusterHelper.isExist(ddlLockPath)) && ddlHelper.getChildrenSize(ddlLockPath) > 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                if (times % 60 == 0) {
                    LOGGER.warn("waiting for DDL finished ");
                    times = 0;
                }
                times++;
            }

            times = 0;
            String viewLockPath = ClusterPathUtil.getViewLockPath();
            final ClusterHelper viewHelper = ClusterHelper.getInstance(ClusterOperation.VIEW);
            while (!Boolean.TRUE.equals(ClusterHelper.isExist(viewLockPath)) && viewHelper.getChildrenSize(viewLockPath) > 0) {
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
                new DDLChildListener().registerPrefixForZk();
                //add tow ha status && ha lock watcher
                if (ClusterConfig.getInstance().isNeedSyncHa()) {
                    new DbGroupStatusListener().registerPrefixForZk();
                    new DbGroupResponseListener().registerPrefixForZk();
                }
                //add watcher
                new ViewChildListener().registerPrefixForZk();
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
     * init meta when OBsharding-D server started
     * no interrupted ,init the view anyway
     *
     * @param config
     */
    private void initMeta(ServerConfig config) {
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
        for (SchemaConfig schema : OBsharding_DServer.getInstance().getConfig().getSchemas().values()) {
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

    public Repository getRepository() {
        return repository;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
