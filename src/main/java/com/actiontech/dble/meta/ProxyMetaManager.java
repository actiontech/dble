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
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.view.CKVStoreRepository;
import com.actiontech.dble.backend.mysql.view.FileSystemRepository;
import com.actiontech.dble.backend.mysql.view.KVStoreRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.zktoxml.listen.DataHostResponseListener;
import com.actiontech.dble.config.loader.zkprocess.zktoxml.listen.DataHostStatusListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.model.DataSourceConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.meta.table.AbstractSchemaMetaHandler;
import com.actiontech.dble.meta.table.DDLNotifyTableMetaHandler;
import com.actiontech.dble.meta.table.SchemaCheckMetaHandler;
import com.actiontech.dble.meta.table.ServerMetaHandler;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.DistrbtLockManager;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.ZKUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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
    private long timestamp;

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
        StringBuffer result = new StringBuffer();
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

    public void addTable(String schema, StructureMeta.TableMeta tm) {
        String tbName = tm.getTableName();
        SchemaMeta schemaMeta = catalogs.get(schema);
        if (schemaMeta != null) {
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
    private boolean dropTable(String schema, String table, String sql, boolean isSuccess, boolean needNotifyOther) {
        if (isSuccess) {
            dropTable(schema, table);
        }
        try {
            notifyResponseClusterDDL(schema, table, sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.DROP_TABLE, needNotifyOther);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schema, table);
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

    public PlanNode getSyncView(String schema, String vName) throws SQLNonTransientException {
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
    }

    private StructureMeta.TableMeta getTableMeta(String schema, String tbName) {
        return catalogs.get(schema).getTableMeta(tbName);
    }

    private Set<String> getSelfNodes(ServerConfig config) {
        Set<String> selfNode = null;
        for (Map.Entry<String, PhysicalDataHost> entry : config.getDataHosts().entrySet()) {
            PhysicalDataHost host = entry.getValue();
            DataSourceConfig wHost = host.getWriteSource().getConfig();
            if (("localhost".equalsIgnoreCase(wHost.getIp()) || "127.0.0.1".equalsIgnoreCase(wHost.getIp())) && wHost.getPort() == config.getSystem().getServerPort()) {
                for (Map.Entry<String, PhysicalDataNode> nodeEntry : config.getDataNodes().entrySet()) {
                    if (nodeEntry.getValue().getDataHost().getHostName().equals(host.getHostName())) {
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
        if (ClusterGeneralConfig.isUseZK()) {
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
        tryDeleteOldOnline();

        // online
        ZKUtils.createOnline(KVPathUtil.getOnlinePath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), OnlineStatus.getInstance());
        //add watcher
        ZKUtils.addChildPathCache(ddlPath, new DDLChildListener());
        //add tow ha status && ha lock watcher
        if (ClusterHelper.useClusterHa()) {
            ZKUtils.addChildPathCache(KVPathUtil.getHaStatusPath(), new DataHostStatusListener());
            ZKUtils.addChildPathCache(KVPathUtil.getHaResponsePath(), new DataHostResponseListener());
        }
        //add watcher
        ZKUtils.addViewPathCache(KVPathUtil.getViewPath(), new ViewChildListener());
        // syncMeta UNLOCK
        zkConn.delete().forPath(KVPathUtil.getSyncMetaLockPath());
    }

    private void tryDeleteOldOnline() throws Exception {
        //try to delete online
        if (ZKUtils.getConnection().checkExists().forPath(KVPathUtil.getOnlinePath() +
                KVPathUtil.SEPARATOR + ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)) != null) {
            byte[] info;
            try {
                info = ZKUtils.getConnection().getData().forPath(KVPathUtil.getOnlinePath() +
                        KVPathUtil.SEPARATOR + ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
            } catch (Exception e) {
                LOGGER.info("can not get old online from zk,just do as it not exists");
                return;
            }
            String oldOnlne = new String(info, StandardCharsets.UTF_8);
            if (OnlineStatus.getInstance().canRemovePath(oldOnlne)) {
                LOGGER.warn("remove online from zk path ,because has same IP & serverPort");
                ZKUtils.getConnection().delete().forPath(KVPathUtil.getOnlinePath() +
                        KVPathUtil.SEPARATOR + ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
            } else {
                throw new RuntimeException("Online path with other IP or serverPort exist,make sure different instance has different myid");
            }
        }
    }

    private void initViewMeta() {
        if (ClusterGeneralConfig.isUseZK()) {
            loadViewFromKV();
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            loadViewFromCKV();
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
                try {
                    ViewMeta vm = new ViewMeta(schemaName.getKey(), view.getValue(), this);
                    vm.init(true);
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
                    vm.init(true);
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
        SystemConfig system = config.getSystem();
        if (system.getCheckTableConsistency() == 1) {
            scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetaDataChecker-%d").build());
            checkTaskHandler = scheduler.scheduleWithFixedDelay(tableStructureCheckTask(selfNode), system.getCheckTableConsistencyPeriod(), system.getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
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
            SystemConfig system = config.getSystem();
            if (system.getCheckTableConsistency() == 1) {
                scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetaDataChecker-%d").build());
                checkTaskHandler = scheduler.scheduleWithFixedDelay(tableStructureCheckTask(selfNode), system.getCheckTableConsistencyPeriod(), system.getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
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
            Map<String, Set<String>> dataNodeMap = new HashMap<>();
            for (Map.Entry<String, TableConfig> entry : schema.getTables().entrySet()) {
                String tableName = entry.getKey();
                TableConfig tbConfig = entry.getValue();
                for (String dataNode : tbConfig.getDataNodes()) {
                    Set<String> tables = dataNodeMap.computeIfAbsent(dataNode, k -> new HashSet<>());
                    tables.add(tableName);
                }
            }

            AbstractSchemaMetaHandler multiTablesMetaHandler = new SchemaCheckMetaHandler(this, schema, selfNode);
            multiTablesMetaHandler.execute();
        }
    }

    public boolean updateMetaData(String schema, String tableName, String sql, boolean isSuccess, boolean needNotifyOther, DDLInfo.DDLType ddlType) {
        if (ddlType == DDLInfo.DDLType.DROP_TABLE) {
            return dropTable(schema, tableName, sql, isSuccess, needNotifyOther);
        } else if (ddlType == DDLInfo.DDLType.TRUNCATE_TABLE) {
            return truncateTable(schema, tableName, sql, isSuccess, needNotifyOther);
        } else if (ddlType == DDLInfo.DDLType.CREATE_TABLE) {
            return createTable(schema, tableName, sql, isSuccess, needNotifyOther);
        } else {
            return generalDDL(schema, tableName, sql, isSuccess, needNotifyOther);
        }
    }

    public void notifyClusterDDL(String schema, String table, String sql) throws Exception {
        if (ClusterGeneralConfig.isUseZK()) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            DDLInfo ddlInfo = new DDLInfo(schema, sql, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), DDLInfo.DDLStatus.INIT, DDLInfo.DDLType.UNKNOWN);
            String nodeName = StringUtil.getFullName(schema, table);
            String nodePath = ZKPaths.makePath(KVPathUtil.getDDLPath(), nodeName);
            zkConn.create().forPath(nodePath, ddlInfo.toString().getBytes(StandardCharsets.UTF_8));
            ClusterDelayProvider.delayAfterDdlLockMeta();
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            DDLInfo ddlInfo = new DDLInfo(schema, sql, ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), DDLInfo.DDLStatus.INIT, DDLInfo.DDLType.UNKNOWN);
            String nodeName = StringUtil.getUFullName(schema, table);
            String ddlLockPath = ClusterPathUtil.getDDLLockPath(nodeName);
            DistributeLock lock = new DistributeLock(ddlLockPath, ddlInfo.toString());
            if (!lock.acquire()) {
                String msg = "The metaLock about `" + nodeName + "` is exists. It means other instance is doing DDL.";
                LOGGER.info(msg + " The path of DDL is " + ddlLockPath);
                throw new Exception(msg);
            }
            ClusterDelayProvider.delayAfterDdlLockMeta();
            DistrbtLockManager.addLock(lock);
            ClusterHelper.setKV(ClusterPathUtil.getDDLPath(nodeName), ddlInfo.toString());
        }
    }

    public void notifyResponseClusterDDL(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType, boolean needNotifyOther) throws Exception {
        ClusterDelayProvider.delayAfterDdlExecuted();
        if (ClusterGeneralConfig.isUseZK()) {
            notifyResponseZKDdl(schema, table, sql, ddlStatus, ddlType, needNotifyOther);
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            notifyResponseUcoreDDL(schema, table, sql, ddlStatus, ddlType, needNotifyOther);
        }
    }

    private void notifyResponseZKDdl(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType, boolean needNotifyOther) throws Exception {
        String nodeName = StringUtil.getFullName(schema, table);
        String nodePath = ZKPaths.makePath(KVPathUtil.getDDLPath(), nodeName);
        String instancePath = ZKPaths.makePath(nodePath, KVPathUtil.DDL_INSTANCE);
        String thisNode = ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
        ZKUtils.createTempNode(instancePath, thisNode);

        if (needNotifyOther) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            DDLInfo ddlInfo = new DDLInfo(schema, sql, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ddlStatus, ddlType);
            ClusterDelayProvider.delayBeforeDdlNotice();
            zkConn.setData().forPath(nodePath, ddlInfo.toString().getBytes(StandardCharsets.UTF_8));
            ClusterDelayProvider.delayAfterDdlNotice();
            while (true) {
                List<String> preparedList = zkConn.getChildren().forPath(instancePath);
                List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                if (preparedList.size() >= onlineList.size()) {
                    ClusterDelayProvider.delayBeforeDdlNoticeDeleted();
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
    public void notifyResponseUcoreDDL(String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType, boolean needNotifyOther) throws Exception {
        String nodeName = StringUtil.getUFullName(schema, table);
        DDLInfo ddlInfo = new DDLInfo(schema, sql, ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ddlStatus, ddlType);
        ClusterHelper.setKV(ClusterPathUtil.getDDLInstancePath(nodeName), ClusterPathUtil.SUCCESS);
        if (needNotifyOther) {
            try {
                ClusterDelayProvider.delayBeforeDdlNotice();
                ClusterHelper.setKV(ClusterPathUtil.getDDLPath(nodeName), ddlInfo.toString());
                ClusterDelayProvider.delayAfterDdlNotice();

                String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getDDLPath(nodeName));

                if (errorMsg != null) {
                    throw new RuntimeException(errorMsg);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                ClusterDelayProvider.delayBeforeDdlNoticeDeleted();
                ClusterHelper.cleanPath(ClusterPathUtil.getDDLPath(nodeName));
                //release the lock
                ClusterDelayProvider.delayBeforeDdlLockRelease();
                DistrbtLockManager.releaseLock(ClusterPathUtil.getDDLLockPath(nodeName));
            }
        }

    }

    private boolean createTable(String schema, String table, String sql, boolean isSuccess, boolean needNotifyOther) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, table);
        boolean result = isSuccess;
        if (isSuccess) {
            String tableName = table;
            TableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(tableName);
            String showDataNode = schemaInfo.getSchemaConfig().getDataNode();
            if (tbConfig != null) {
                for (String dataNode : tbConfig.getDataNodes()) {
                    showDataNode = dataNode;
                    String tableId = "DataNode[" + dataNode + "]:Table[" + tableName + "]";
                    if (ToResolveContainer.TABLE_LACK.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId), ToResolveContainer.TABLE_LACK, tableId);
                    }
                }
            }
            DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schema, tableName, Collections.singletonList(showDataNode), null);
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


    private boolean truncateTable(String schema, String table, String sql, boolean isSuccess, boolean needNotifyOther) {
        try {
            notifyResponseClusterDDL(schema, table, sql, isSuccess ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, DDLInfo.DDLType.TRUNCATE_TABLE, needNotifyOther);
        } catch (Exception e) {
            LOGGER.warn("notifyResponseClusterDDL error", e);
        }
        removeMetaLock(schema, table);
        return true;
    }


    private boolean generalDDL(String schema, String table, String sql, boolean isSuccess, boolean needNotifyOther) {
        SchemaInfo schemaInfo = getSchemaInfo(schema, table);
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
