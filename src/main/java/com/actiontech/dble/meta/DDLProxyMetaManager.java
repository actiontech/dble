package com.actiontech.dble.meta;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.DistributeLockManager;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.path.PathMeta;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.meta.table.DDLNotifyTableMetaHandler;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.DDLTraceHelper;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class DDLProxyMetaManager {

    private DDLProxyMetaManager() {
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(DDLProxyMetaManager.class);

    public static void removeLocalMetaLock(String schema, String table) {
        ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
    }

    // instance of ddl is triggered
    public static class Originator {

        public static void notifyClusterDDLPrepare(ShardingService shardingService, String schema, String table, String sql) throws Exception {
            try {
                if (ClusterConfig.getInstance().isClusterEnable()) {
                    DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_prepare, DDLTraceHelper.Status.start, "Notify and wait for all instances to enter phase PREPARE"));
                    if (DistributeLockManager.isLooked(ClusterPathUtil.getSyncMetaLockPath())) {
                        String msg = "Found another instance initializing metadata. Please try later.";
                        throw new Exception(msg);
                    }
                    DDLInfo ddlInfo = new DDLInfo(schema, sql, SystemConfig.getInstance().getInstanceName(), DDLInfo.DDLStatus.INIT, DDLInfo.DDLType.UNKNOWN);
                    String tableFullName = StringUtil.getUFullName(schema, table);
                    final PathMeta<DDLInfo> ddlLockPathMeta = ClusterMetaUtil.getDDLLockPath(tableFullName);
                    ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.DDL);
                    DistributeLock lock = clusterHelper.createDistributeLock(ddlLockPathMeta, ddlInfo);
                    if (!lock.acquire()) {
                        DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_prepare, "Failed to acquire table[" + tableFullName + "]’s ddlLock，the ddlLock's path is " + ddlLockPathMeta));
                        String msg = "Found another instance doing ddl, duo to table[" + tableFullName + "]’s ddlLock is exists.";
                        throw new Exception(msg);
                    }
                    DistributeLockManager.addLock(lock);
                    ClusterDelayProvider.delayAfterDdlLockMeta();
                    final PathMeta<DDLInfo> ddlPathMeta = ClusterMetaUtil.getDDLPath(tableFullName, DDLInfo.NodeStatus.PREPARE);
                    clusterHelper.setKV(ddlPathMeta, ddlInfo);
                    clusterHelper.createSelfTempNode(ddlPathMeta.getPath(), FeedBackType.SUCCESS);
                    String errorMsg = ClusterLogic.forDDL().waitingForAllTheNode(ddlPathMeta.getPath());
                    if (errorMsg != null) {
                        throw new RuntimeException("Some instances have an accident at phase PREPARE. err: " + errorMsg);
                    }
                    DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_prepare, DDLTraceHelper.Status.succ, "All instances have entered phase PREPARE"));
                }
            } catch (Exception e) {
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_prepare, DDLTraceHelper.Status.fail, e.getMessage()));
                throw e;
            }
        }

        public static void addLocalMetaLock(ShardingService shardingService, String schema, String table, String sql) throws SQLNonTransientException {
            try {
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.add_local_lock, DDLTraceHelper.Status.start));
                ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, sql);
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.add_local_lock, DDLTraceHelper.Status.succ));
            } catch (Exception e) {
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.add_local_lock, DDLTraceHelper.Status.fail, e.getMessage()));
                throw e;
            }
        }

        // consists of two parts: updating meta and cluster notify
        public static boolean updateMetaData(ShardingService shardingService, RouteResultset rrs, boolean isExecSucc) {
            ProxyMetaManager proxyMetaManager = ProxyMeta.getInstance().getTmManager();
            String schema = rrs.getSchema();
            String table = rrs.getTable();
            String sql = rrs.getSrcStatement();

            boolean isUpdateSucc = true;
            // 1.local metadata update
            if (!isExecSucc) {
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_ddl_metadata, "The table[" + schema + "." + table + "]’s metadata is not updated because ddl execution failed"));
            } else {
                switch (rrs.getDdlType()) {
                    case DROP_TABLE:
                        DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.start));
                        proxyMetaManager.dropTable(schema, table);
                        DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.succ));
                        break;
                    case TRUNCATE_TABLE:
                        // no processing
                        DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_ddl_metadata, "DDLType is TRUNCATE_TABLE, no need to update metadata"));
                        break;
                    case CREATE_TABLE:
                        isUpdateSucc = createTable(shardingService, schema, table);
                        break;
                    default:
                        isUpdateSucc = generalDDL(shardingService, schema, table);
                        break;
                }
            }

            // 2.pushing to the cluster
            try {
                notifyClusterDDLComplete(proxyMetaManager, shardingService, schema, table, sql, isExecSucc ? DDLInfo.DDLStatus.SUCCESS : DDLInfo.DDLStatus.FAILED, rrs.getDdlType());
            } catch (Exception e) {
                LOGGER.warn("notifyClusterDDLComplete error: {}", e);
            }
            proxyMetaManager.removeMetaLock(schema, table);
            return isUpdateSucc;
        }

        private static boolean createTable(ShardingService shardingService, String schema, String table) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfoWithoutCheck(schema, table);
            BaseTableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(table);
            String showShardingNode = null;
            List<String> shardingNodes;
            if (tbConfig != null) {
                shardingNodes = tbConfig.getShardingNodes();
            } else {
                shardingNodes = schemaInfo.getSchemaConfig().getDefaultShardingNodes();
            }
            for (String shardingNode : shardingNodes) {
                showShardingNode = shardingNode;
                String tableLackKey = AlertUtil.getTableLackKey(shardingNode, table);
                if (ToResolveContainer.TABLE_LACK.contains(tableLackKey)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableLackKey), ToResolveContainer.TABLE_LACK, tableLackKey);
                }
            }
            DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.start)); // end to DDLNotifyTableMetaHandler.handlerTable
            DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schema, table, Collections.singletonList(showShardingNode), null, true, shardingService);
            handler.execute();
            return handler.isMetaInited();
        }

        private static boolean generalDDL(ShardingService shardingService, String schema, String table) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfoWithoutCheck(schema, table);
            BaseTableConfig tbConfig = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
            String showShardingNode = null;
            if (tbConfig != null) {
                for (String shardingNode : tbConfig.getShardingNodes()) {
                    showShardingNode = shardingNode;
                    break;
                }
            } else {
                showShardingNode = schemaInfo.getSchemaConfig().getDefaultShardingNodes().get(0); // randomly take a shardingNode
            }
            DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.start)); // end to DDLNotifyTableMetaHandler.handlerTable
            DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schemaInfo.getSchema(), schemaInfo.getTable(), Collections.singletonList(showShardingNode), null, false, shardingService);
            handler.execute();
            return handler.isMetaInited();
        }

        private static void notifyClusterDDLComplete(ProxyMetaManager proxyMetaManager, ShardingService shardingService,
                                                     String schema, String table, String sql, DDLInfo.DDLStatus ddlStatus, DDLInfo.DDLType ddlType) throws Exception {
            ClusterDelayProvider.delayAfterDdlExecuted();
            if (ClusterConfig.getInstance().isClusterEnable()) {
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_complete, DDLTraceHelper.Status.start, "Notify and wait for all instances to enter phase COMPLETE"));
                String tableFullName = StringUtil.getUFullName(schema, table);
                final PathMeta<DDLInfo> tableDDLPath = ClusterMetaUtil.getDDLPath(tableFullName, DDLInfo.NodeStatus.COMPLETE);
                ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.DDL);
                boolean isLock = true;
                proxyMetaManager.getMetaLock().lock();
                try {
                    if (proxyMetaManager.getLockTables().containsKey(schema + "." + table)) {
                        ClusterDelayProvider.delayBeforeDdlNotice();
                        DDLInfo ddlInfo = new DDLInfo(schema, sql, SystemConfig.getInstance().getInstanceName(), ddlStatus, ddlType);
                        clusterHelper.setKV(tableDDLPath, ddlInfo);
                        ClusterHelper.cleanPath(ClusterMetaUtil.getDDLPath(tableFullName, DDLInfo.NodeStatus.PREPARE));
                        ClusterDelayProvider.delayAfterDdlNotice();
                        clusterHelper.createSelfTempNode(tableDDLPath.getPath(), FeedBackType.SUCCESS);
                        proxyMetaManager.getMetaLock().unlock();
                        isLock = false;
                        String errorMsg = ClusterLogic.forDDL().waitingForAllTheNode(tableDDLPath.getPath());
                        if (errorMsg != null) {
                            String msg = "Some instances have an accident at phase COMPLETE. err: " + errorMsg;
                            DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_complete, DDLTraceHelper.Status.fail, msg));
                            throw new RuntimeException(msg);
                        }
                        DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.notice_cluster_ddl_complete, DDLTraceHelper.Status.succ, "All instances have entered phase COMPLETE"));
                    } else {
                        proxyMetaManager.getMetaLock().unlock();
                        isLock = false;
                    }
                } finally {
                    if (isLock) {
                        proxyMetaManager.getMetaLock().unlock();
                    }
                    ClusterDelayProvider.delayBeforeDdlNoticeDeleted();
                    ClusterHelper.cleanPath(tableDDLPath);
                    //release the lock
                    ClusterDelayProvider.delayBeforeDdlLockRelease();
                    DistributeLockManager.releaseLock(ClusterPathUtil.getDDLLockPath(tableFullName));
                }
            }
        }
    }

    // instances of being awakened by ddl
    public static class Subscriber {

        public static void addLocalMetaLock(String schema, String table, String sql) throws SQLNonTransientException {
            try {
                DDLTraceHelper.log2(null, DDLTraceHelper.Stage.add_local_lock, DDLTraceHelper.Status.start);
                ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, sql);
                DDLTraceHelper.log2(null, DDLTraceHelper.Stage.add_local_lock, DDLTraceHelper.Status.succ);
            } catch (Exception e) {
                DDLTraceHelper.log2(null, DDLTraceHelper.Stage.add_local_lock, DDLTraceHelper.Status.fail, e.getMessage());
                throw e;
            }
        }

        // consists of two parts: updating meta and release metaLock
        public static boolean updateMetaData(String schema, String table, @Nullable DDLInfo ddlInfo, boolean isExecSucc) {
            ProxyMetaManager proxyMetaManager = ProxyMeta.getInstance().getTmManager();
            if (!isExecSucc) {
                DDLTraceHelper.log2(null, DDLTraceHelper.Stage.update_ddl_metadata, "The table[" + schema + "." + table + "]’s metadata is not updated because ddl execution failed");
            } else {
                switch (ddlInfo.getType()) {
                    case DROP_TABLE:
                        if (DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus())) {
                            DDLTraceHelper.log2(null, DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.start);
                            proxyMetaManager.dropTable(schema, table);
                            DDLTraceHelper.log2(null, DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.succ);
                        }
                        break;
                    default:
                        updateTableWithBackData(schema, table, ddlInfo.getType() == DDLInfo.DDLType.CREATE_TABLE);
                        break;
                }
            }
            proxyMetaManager.removeMetaLock(schema, table);
            return true;
        }

        private static void updateTableWithBackData(String schema, String table, boolean isCreateSql) {
            ServerConfig currConfig = DbleServer.getInstance().getConfig();
            Set<String> selfNode = ProxyMetaManager.getSelfNodes(currConfig);
            List<String> shardingNodes;
            if (currConfig.getSchemas().get(schema).getTables().get(table) == null) {
                shardingNodes = currConfig.getSchemas().get(schema).getDefaultShardingNodes();
            } else {
                shardingNodes = currConfig.getSchemas().get(schema).getTables().get(table).getShardingNodes();
            }
            DDLTraceHelper.log2(null, DDLTraceHelper.Stage.update_ddl_metadata, DDLTraceHelper.Status.start); // end to DDLNotifyTableMetaHandler.handlerTable
            DDLNotifyTableMetaHandler handler = new DDLNotifyTableMetaHandler(schema, table, shardingNodes, selfNode, isCreateSql, null);
            handler.execute();
        }
    }
}
