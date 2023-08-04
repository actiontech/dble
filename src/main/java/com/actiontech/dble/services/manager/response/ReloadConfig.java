/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ApNode;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.config.ConfigInitializer;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.meta.ReloadException;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.ManagerParseConfig;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.CronScheduler;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.cluster.path.ClusterPathUtil.SEPARATOR;
import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_COMMAND;
import static com.actiontech.dble.services.manager.response.ChangeItemType.PHYSICAL_DB_INSTANCE;

public final class ReloadConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadConfig.class);

    private ReloadConfig() {
    }

    public static void execute(ManagerService service, String stmt, int offset) {
        try {
            ManagerParseConfig parser = new ManagerParseConfig();
            int rs = parser.parse(stmt, offset);
            switch (rs) {
                case ManagerParseConfig.CONFIG:
                case ManagerParseConfig.CONFIG_ALL:
                    ReloadConfig.execute(service, parser.getMode(), true, new ConfStatus(ConfStatus.Status.RELOAD_ALL));
                    writePacket(true, service, null, 0);
                    break;
                default:
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                    break;
            }
        } catch (ReloadException e) {
            writePacket(false, service, e.getMessage() == null ? e.toString() : e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            writePacket(false, service, e.getMessage() == null ? e.toString() : e.getMessage(),
                    (e.getCause() instanceof ReloadException) ? ((ReloadException) e.getCause()).getErrorCode() : ErrorCode.ER_YES);
        }
    }

    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus) throws Exception {
        try {
            if (ClusterConfig.getInstance().isClusterEnable()) {
                reloadWithCluster(service, loadAllMode, confStatus);
            } else {
                reloadWithoutCluster(service, loadAllMode, returnFlag, confStatus);
            }
        } catch (ReloadException e) {
            ReloadLogHelper.warn2("Reload config failure. The reason is {}", e.getMessage());
            throw e;
        } catch (ConfigException e) {
            ReloadLogHelper.warn2("Reload config failure. The reason is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof ReloadException || e.getCause() instanceof ConfigException) {
                ReloadLogHelper.warn2("Reload config failure. The reason is {}", e.getMessage());
            } else {
                ReloadLogHelper.warn2("Reload config failure. catch exception is ", e);
            }
            throw e;
        } finally {
            ReloadManager.reloadFinish();
        }
    }

    private static void reloadWithCluster(ManagerService service, int loadAllMode, ConfStatus confStatus) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-with-cluster");
        try {

            ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
            DistributeLock distributeLock = null;
            if (!confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) && !confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) &&
                    !confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getConfChangeLockPath());
                if (!distributeLock.acquire()) {
                    throw new ReloadException(ErrorCode.ER_YES, "Other instance is reloading, please try again later.");
                }
                ReloadLogHelper.graceInfo("added distributeLock, path" + ClusterMetaUtil.getConfChangeLockPath());
            }
            try {
                ClusterDelayProvider.delayAfterReloadLock();
                if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                    throw new ReloadException(ErrorCode.ER_YES, "reload status error, other client or cluster may in reload");
                }
                //step 1 lock the local meta ,than all the query depends on meta will be hanging
                final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
                lock.writeLock().lock();
                try {
                    ReloadLogHelper.briefInfo("added configLock");
                    //step 2 reload the local config file
                    boolean reloadResult;
                    if (confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) || confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) ||
                            confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                        reloadResult = reloadByConfig(loadAllMode, true);
                    } else {
                        reloadResult = reloadByLocalXml(loadAllMode);
                    }
                    if (!reloadResult) {
                        throw new ReloadException(ErrorCode.ER_RELOAD_INTERRUPUTED, "reload interruputed by others, config should be reload");
                    }
                    ReloadLogHelper.briefInfo("single instance(self) finished");
                    ClusterDelayProvider.delayAfterMasterLoad();

                    //step 3 if the reload with no error ,than write the config file into cluster center remote
                    ClusterHelper.writeConfToCluster();
                    ReloadLogHelper.briefInfo("sent config file to cluster center");

                    //step 4 write the reload flag and self reload result into cluster center,notify the other dble to reload
                    ConfStatus status = new ConfStatus(SystemConfig.getInstance().getInstanceName(),
                            ConfStatus.Status.RELOAD_ALL, String.valueOf(loadAllMode));
                    clusterHelper.setKV(ClusterMetaUtil.getConfStatusOperatorPath(), status);
                    ReloadLogHelper.briefInfo("sent config status to cluster center");
                    //step 5 start a loop to check if all the dble in cluster is reload finished
                    ReloadManager.waitingOthers();
                    clusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), FeedBackType.SUCCESS);
                    final String errorMsg = ClusterLogic.forConfig().waitingForAllTheNode(ClusterPathUtil.getConfStatusOperatorPath());
                    ReloadLogHelper.briefInfo("all instances finished");
                    ClusterDelayProvider.delayBeforeDeleteReloadLock();

                    if (errorMsg != null) {
                        throw new ReloadException(errorMsg.contains("interrupt by command") ? ErrorCode.ER_RELOAD_INTERRUPUTED : ErrorCode.ER_CLUSTER_RELOAD,
                                "partial instance reload failed, failed because of:[" + errorMsg + "]");
                    }
                } finally {
                    lock.writeLock().unlock();
                    ClusterHelper.cleanPath(ClusterPathUtil.getConfStatusOperatorPath() + SEPARATOR);
                    ReloadLogHelper.briefInfo("released configLock");
                }
            } finally {
                if (distributeLock != null) {
                    distributeLock.release();
                    ReloadLogHelper.briefInfo("released distributeLock");
                }
            }
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }
    }

    private static void reloadWithoutCluster(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus) throws ReloadException, Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-in-local");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            ReloadLogHelper.graceInfo("added configLock");
            if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                throw new ReloadException(ErrorCode.ER_YES, "reload status error ,other client or cluster may in reload");
            }
            boolean reloadResult;
            if (confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) || confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) ||
                    confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                reloadResult = reloadByConfig(loadAllMode, true);
            } else {
                reloadResult = reloadByLocalXml(loadAllMode);
            }
            if (reloadResult && returnFlag) {
                // ok package
                return;
            } else if (!reloadResult) {
                throw new ReloadException(ErrorCode.ER_RELOAD_INTERRUPUTED, "reload interruputed by others,metadata should be reload");
            }
        } finally {
            lock.writeLock().unlock();
            ReloadLogHelper.briefInfo("released configLock");
            TraceManager.finishSpan(service, traceObject);
        }
    }


    private static void writeErrorResult(ManagerService c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static boolean reloadByLocalXml(final int loadAllMode) throws Exception {
        return reload(loadAllMode, null, null, null, null);
    }

    public static boolean reloadByConfig(final int loadAllMode, boolean isWriteToLocal) throws Exception {
        RawJson userConfig = DbleTempConfig.getInstance().getUserConfig();
        userConfig = userConfig == null ? DbleServer.getInstance().getConfig().getUserConfig() : userConfig;
        RawJson dbConfig = DbleTempConfig.getInstance().getDbConfig();
        dbConfig = dbConfig == null ? DbleServer.getInstance().getConfig().getDbConfig() : dbConfig;
        RawJson shardingConfig = DbleTempConfig.getInstance().getShardingConfig();
        shardingConfig = shardingConfig == null ? DbleServer.getInstance().getConfig().getShardingConfig() : shardingConfig;
        RawJson sequenceConfig = DbleTempConfig.getInstance().getSequenceConfig();
        sequenceConfig = sequenceConfig == null ? DbleServer.getInstance().getConfig().getSequenceConfig() : sequenceConfig;
        final boolean reloadResult = reload(loadAllMode, userConfig, dbConfig, shardingConfig, sequenceConfig);

        ReloadLogHelper.briefInfo("clean temp config ...");
        DbleTempConfig.getInstance().clean();
        //sync json to local
        if (isWriteToLocal)
            ReloadLogHelper.briefInfo("sync json to local ...");
        DbleServer.getInstance().getConfig().syncJsonToLocal(isWriteToLocal);
        return reloadResult;
    }

    private static boolean reload(final int loadAllMode, RawJson userConfig, RawJson dbConfig, RawJson shardingConfig, RawJson sequenceConfig) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("self-reload");
        try {
            // load configuration
            ConfigInitializer loader = loadConfig(userConfig, dbConfig, shardingConfig, sequenceConfig);

            // compare changes
            List<ChangeItem> changeItemList = compareChange(loader);

            boolean forceAllReload = false;
            if ((loadAllMode & ManagerParseConfig.OPTR_MODE) != 0) {
                //-r
                forceAllReload = true;
            }

            // test connection
            testConnection(loader, changeItemList, forceAllReload, loadAllMode);

            ServerConfig newConfig = new ServerConfig(loader);
            Map<String, PhysicalDbGroup> newDbGroups = newConfig.getDbGroups();

            // check version/packetSize/lowerCase && get system variables
            SystemVariables newSystemVariables = checkVersionAGetSystemVariables(loader, newDbGroups, changeItemList, forceAllReload);

            // recycle old active conn
            recycleOldBackendConnections(forceAllReload, (loadAllMode & ManagerParseConfig.OPTF_MODE) != 0);


            // lowerCase
            if (loader.isFullyConfigured()) {
                if (newSystemVariables.isLowerCaseTableNames()) {
                    ReloadLogHelper.briefInfo("dbGroup's lowerCaseTableNames=1, lower the config properties ...");
                    newConfig.reviseLowerCase();
                }
                ReloadLogHelper.briefInfo("selfChecking0 ...");
                newConfig.selfChecking0();
            }
            // load sequence
            newConfig.reloadSequence(loader.getSequenceConfig());

            Map<UserName, UserConfig> newUsers = newConfig.getUsers();
            Map<String, SchemaConfig> newSchemas = newConfig.getSchemas();
            Map<String, ShardingNode> newShardingNodes = newConfig.getShardingNodes();
            Map<String, ApNode> newApNodes = newConfig.getApNodes();
            Map<ERTable, Set<ERTable>> newErRelations = newConfig.getErRelations();
            Map<String, Set<ERTable>> newFuncNodeERMap = newConfig.getFuncNodeERMap();
            Map<String, Properties> newBlacklistConfig = newConfig.getBlacklistConfig();
            Map<String, AbstractPartitionAlgorithm> newFunctions = newConfig.getFunctions();


            // start/stop connection pool && heartbeat
            // replace config
            ReloadLogHelper.briefInfo("apply new config start");
            ServerConfig oldConfig = DbleServer.getInstance().getConfig();
            boolean result;
            try {
                result = oldConfig.reload(newUsers, newSchemas, newShardingNodes, newApNodes, newDbGroups, oldConfig.getDbGroups(), newErRelations, newFuncNodeERMap,
                        newSystemVariables, loader.isFullyConfigured(), loadAllMode, newBlacklistConfig, newFunctions,
                        loader.getUserConfig(), loader.getSequenceConfig(), loader.getShardingConfig(), loader.getDbConfig(), changeItemList);
                CronScheduler.getInstance().init(oldConfig.getSchemas());
                if (!result) {
                    initFailed(newDbGroups);
                }
                FrontendUserManager.getInstance().changeUser(changeItemList, SystemConfig.getInstance().getMaxCon());
                ReloadLogHelper.briefInfo("apply new config end");
                // recycle old active conn
                recycleOldBackendConnections(!forceAllReload, (loadAllMode & ManagerParseConfig.OPTF_MODE) != 0);
                if (!loader.isFullyConfigured()) {
                    recycleServerConnections();
                }
                return result;
            } catch (Exception e) {
                initFailed(newDbGroups);
                throw e;
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    /**
     * check version/packetSize/lowerCase
     * get system variables
     */
    private static SystemVariables checkVersionAGetSystemVariables(ConfigInitializer loader, Map<String, PhysicalDbGroup> newDbGroups, List<ChangeItem> changeItemList, boolean forceAllReload) throws Exception {
        ReloadLogHelper.briefInfo("check and get system variables from random node start");
        SystemVariables newSystemVariables;
        if (forceAllReload || ConfigUtil.isAllDbInstancesChange(changeItemList)) {
            //check version
            ConfigUtil.checkDbleAndMysqlVersion(newDbGroups);
            //check packetSize/lowerCase
            ConfigUtil.getAndSyncKeyVariables(newDbGroups, true);
            //get system variables
            newSystemVariables = getSystemVariablesFromdbGroup(loader, newDbGroups);
        } else {
            //check version
            ConfigUtil.checkDbleAndMysqlVersion(changeItemList, loader);
            //check packetSize/lowerCase
            ConfigUtil.getAndSyncKeyVariables(changeItemList, true);
            //keep the original system variables
            newSystemVariables = DbleServer.getInstance().getSystemVariables();
        }
        ReloadLogHelper.briefInfo("check and get system variables from random node end");
        return newSystemVariables;
    }



    /**
     * test connection
     */
    private static void testConnection(ConfigInitializer loader, List<ChangeItem> changeItemList, boolean forceAllReload, int loadAllMode) throws Exception {
        ReloadLogHelper.briefInfo("test connection start");
        try {
            //test connection
            if (forceAllReload && loader.isFullyConfigured()) {
                loader.testConnection();
            } else {
                syncShardingAndApNode(loader);
                loader.testConnection(changeItemList);
            }
        } catch (Exception e) {
            if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 && loader.isFullyConfigured()) {
                //default/-f/-r
                throw new Exception(e);
            } else {
                //-s
                ReloadLogHelper.debug("just test, not stop reload, catch exception", e);
            }
        }
        ReloadLogHelper.briefInfo("test connection end");
    }

    /**
     * compare change
     */
    private static List<ChangeItem> compareChange(ConfigInitializer loader) {
        ReloadLogHelper.briefInfo("compare changes start");
        List<ChangeItem> changeItemList = differentiateChanges(loader);
        ReloadLogHelper.debug("change items :{}", changeItemList);
        ReloadLogHelper.briefInfo("compare changes end");
        return changeItemList;
    }

    /**
     * load configuration
     * xml:db.xml/user.xml/sharding.xml/sequence
     * memory:dble_information/cluster synchronization
     */
    private static ConfigInitializer loadConfig(RawJson userConfig, RawJson dbConfig, RawJson shardingConfig, RawJson sequenceConfig) throws Exception {
        ConfigInitializer loader;
        try {
            if (null == userConfig && null == dbConfig && null == shardingConfig && null == sequenceConfig) {
                ReloadLogHelper.briefInfo("load config start [local xml]");
                loader = new ConfigInitializer();
            } else {
                ReloadLogHelper.briefInfo("load config start [memory]");
                ReloadLogHelper.debug("memory to Users is :{}\r\n" +
                        "memory to DbGroups is :{}\r\n" +
                        "memory to Shardings is :{}\r\n" +
                        "memory to sequence is :{}", userConfig, dbConfig, shardingConfig, sequenceConfig);
                loader = new ConfigInitializer(userConfig, dbConfig, shardingConfig, sequenceConfig);
            }
            ReloadLogHelper.briefInfo("load config end");
            return loader;
        } catch (Exception e) {
            throw new Exception(e.getMessage() == null ? e.toString() : e.getMessage(), e);
        }
    }

    private static void syncShardingAndApNode(ConfigInitializer loader) {
        // sync schema_exists(only testConn can update schema_exists)
        Map<String, ShardingNode> oldShardingNodeMap = DbleServer.getInstance().getConfig().getShardingNodes();
        Map<String, ShardingNode> newShardingNodeMap = loader.getShardingNodes();
        for (Map.Entry<String, ShardingNode> shardingNodeEntry : newShardingNodeMap.entrySet()) {
            if (oldShardingNodeMap.containsKey(shardingNodeEntry.getKey())) {
                shardingNodeEntry.getValue().setSchemaExists(oldShardingNodeMap.get(shardingNodeEntry.getKey()).isSchemaExists());
            }
        }

        Map<String, ApNode> oldApNodeMap = DbleServer.getInstance().getConfig().getApNodes();
        Map<String, ApNode> newApNodeMap = loader.getApNodes();
        for (Map.Entry<String, ApNode> apNodeEntry : newApNodeMap.entrySet()) {
            if (oldApNodeMap.containsKey(apNodeEntry.getKey())) {
                apNodeEntry.getValue().setSchemaExists(oldApNodeMap.get(apNodeEntry.getKey()).isSchemaExists());
            }
        }

    }

    private static List<ChangeItem> differentiateChanges(ConfigInitializer newLoader) {
        List<ChangeItem> changeItemList = Lists.newArrayList();
        //user
        //old
        ServerConfig oldServerConfig = DbleServer.getInstance().getConfig();
        Map<UserName, UserConfig> oldUserMap = oldServerConfig.getUsers();
        //new
        Map<UserName, UserConfig> newUserMap = newLoader.getUsers();
        MapDifference<UserName, UserConfig> userMapDifference = Maps.difference(newUserMap, oldUserMap);
        //delete
        userMapDifference.entriesOnlyOnRight().keySet().stream().map(username -> new ChangeItem(ChangeType.DELETE, username, ChangeItemType.USERNAME)).forEach(changeItemList::add);
        //add
        userMapDifference.entriesOnlyOnLeft().keySet().stream().map(username -> new ChangeItem(ChangeType.ADD, username, ChangeItemType.USERNAME)).forEach(changeItemList::add);
        //update
        userMapDifference.entriesDiffering().entrySet().stream().map(differenceEntry -> {
            UserConfig newUserConfig = differenceEntry.getValue().leftValue();
            UserConfig oldUserConfig = differenceEntry.getValue().rightValue();
            ChangeItem changeItem = new ChangeItem(ChangeType.UPDATE, differenceEntry.getKey(), ChangeItemType.USERNAME);
            if (newUserConfig instanceof RwSplitUserConfig && oldUserConfig instanceof RwSplitUserConfig) {
                if (!((RwSplitUserConfig) newUserConfig).getDbGroup().equals(((RwSplitUserConfig) oldUserConfig).getDbGroup())) {
                    changeItem.setAffectEntryDbGroup(true);
                }
            }
            return changeItem;
        }).forEach(changeItemList::add);

        //shardingNode
        Map<String, ShardingNode> oldShardingNodeMap = oldServerConfig.getShardingNodes();
        Map<String, ShardingNode> newShardingNodeMap = newLoader.getShardingNodes();
        MapDifference<String, ShardingNode> shardingNodeMapDiff = Maps.difference(newShardingNodeMap, oldShardingNodeMap);
        //delete
        shardingNodeMapDiff.entriesOnlyOnRight().values().stream().map(sharingNode -> new ChangeItem(ChangeType.DELETE, sharingNode, ChangeItemType.SHARDING_NODE)).forEach(changeItemList::add);
        //add
        shardingNodeMapDiff.entriesOnlyOnLeft().values().stream().map(sharingNode -> new ChangeItem(ChangeType.ADD, sharingNode, ChangeItemType.SHARDING_NODE)).forEach(changeItemList::add);
        //update
        shardingNodeMapDiff.entriesDiffering().entrySet().stream().map(differenceEntry -> {
            ShardingNode newShardingNode = differenceEntry.getValue().leftValue();
            ChangeItem changeItem = new ChangeItem(ChangeType.UPDATE, newShardingNode, ChangeItemType.SHARDING_NODE);
            return changeItem;
        }).forEach(changeItemList::add);

        //apNode
        Map<String, ApNode> oldApNodeMap = oldServerConfig.getApNodes();
        Map<String, ApNode> newApNodeMap = newLoader.getApNodes();
        MapDifference<String, ApNode> apNodeMapDiff = Maps.difference(newApNodeMap, oldApNodeMap);
        //delete
        apNodeMapDiff.entriesOnlyOnRight().values().stream().map(apNode -> new ChangeItem(ChangeType.DELETE, apNode, ChangeItemType.AP_NODE)).forEach(changeItemList::add);
        //add
        apNodeMapDiff.entriesOnlyOnLeft().values().stream().map(apNode -> new ChangeItem(ChangeType.ADD, apNode, ChangeItemType.AP_NODE)).forEach(changeItemList::add);
        //update
        apNodeMapDiff.entriesDiffering().entrySet().stream().map(differenceEntry -> {
            ApNode newApNode = differenceEntry.getValue().leftValue();
            ChangeItem changeItem = new ChangeItem(ChangeType.UPDATE, newApNode, ChangeItemType.AP_NODE);
            return changeItem;
        }).forEach(changeItemList::add);

        //dbGroup
        Map<String, PhysicalDbGroup> oldDbGroupMap = oldServerConfig.getDbGroups();
        Map<String, PhysicalDbGroup> newDbGroupMap = newLoader.getDbGroups();
        Map<String, PhysicalDbGroup> removeDbGroup = new LinkedHashMap<>(oldDbGroupMap);
        for (Map.Entry<String, PhysicalDbGroup> newDbGroupEntry : newDbGroupMap.entrySet()) {
            PhysicalDbGroup oldDbGroup = oldDbGroupMap.get(newDbGroupEntry.getKey());
            PhysicalDbGroup newDbGroup = newDbGroupEntry.getValue();

            if (null == oldDbGroup) {
                //add dbGroup
                changeItemList.add(new ChangeItem(ChangeType.ADD, newDbGroup, ChangeItemType.PHYSICAL_DB_GROUP));
            } else {
                removeDbGroup.remove(newDbGroupEntry.getKey());
                //change dbGroup
                if (!newDbGroup.equalsBaseInfo(oldDbGroup)) {
                    ChangeItem changeItem = new ChangeItem(ChangeType.UPDATE, newDbGroup, ChangeItemType.PHYSICAL_DB_GROUP);
                    if (!newDbGroup.equalsForConnectionPool(oldDbGroup)) {
                        changeItem.setAffectConnectionPool(true);
                    }
                    if (!newDbGroup.equalsForHeartbeat(oldDbGroup)) {
                        changeItem.setAffectHeartbeat(true);
                    }
                    if (!newDbGroup.equalsForDelayDetection(oldDbGroup)) {
                        changeItem.setAffectDelayDetection(true);
                    }
                    changeItemList.add(changeItem);
                }

                //dbInstance
                Map<String, PhysicalDbInstance> newDbInstanceMap = newDbGroup.getAllDbInstanceMap();
                Map<String, PhysicalDbInstance> oldDbInstanceMap = oldDbGroup.getAllDbInstanceMap();

                MapDifference<String, PhysicalDbInstance> dbInstanceMapDifference = Maps.difference(newDbInstanceMap, oldDbInstanceMap);
                //delete
                dbInstanceMapDifference.entriesOnlyOnRight().values().stream().map(dbInstance -> new ChangeItem(ChangeType.DELETE, dbInstance, PHYSICAL_DB_INSTANCE)).forEach(changeItemList::add);
                //add
                dbInstanceMapDifference.entriesOnlyOnLeft().values().stream().map(dbInstance -> new ChangeItem(ChangeType.ADD, dbInstance, PHYSICAL_DB_INSTANCE)).forEach(changeItemList::add);
                //update
                dbInstanceMapDifference.entriesDiffering().values().stream().map(physicalDbInstanceValueDifference -> {
                    PhysicalDbInstance newDbInstance = physicalDbInstanceValueDifference.leftValue();
                    PhysicalDbInstance oldDbInstance = physicalDbInstanceValueDifference.rightValue();
                    ChangeItem changeItem = new ChangeItem(ChangeType.UPDATE, newDbInstance, PHYSICAL_DB_INSTANCE);
                    if (!newDbInstance.equalsForConnectionPool(oldDbInstance)) {
                        changeItem.setAffectConnectionPool(true);
                    }
                    if (!newDbInstance.equalsForPoolCapacity(oldDbInstance)) {
                        changeItem.setAffectPoolCapacity(true);
                    }
                    if (!newDbInstance.equalsForHeartbeat(oldDbInstance)) {
                        changeItem.setAffectHeartbeat(true);
                    }
                    if (!newDbInstance.equalsForDelayDetection(oldDbInstance)) {
                        changeItem.setAffectDelayDetection(true);
                    }
                    if (!newDbInstance.equalsForTestConn(oldDbInstance)) {
                        changeItem.setAffectTestConn(true);
                    } else {
                        newDbInstance.setTestConnSuccess(oldDbInstance.isTestConnSuccess());
                        newDbInstance.setDsVersion(oldDbInstance.getDsVersion());
                    }
                    return changeItem;
                }).forEach(changeItemList::add);
                //testConnSuccess with both
                for (Map.Entry<String, PhysicalDbInstance> dbInstanceEntry : dbInstanceMapDifference.entriesInCommon().entrySet()) {
                    dbInstanceEntry.getValue().setTestConnSuccess(oldDbInstanceMap.get(dbInstanceEntry.getKey()).isTestConnSuccess());
                    dbInstanceEntry.getValue().setDsVersion(oldDbInstanceMap.get(dbInstanceEntry.getKey()).getDsVersion());
                }

            }
        }
        for (Map.Entry<String, PhysicalDbGroup> entry : removeDbGroup.entrySet()) {
            PhysicalDbGroup value = entry.getValue();
            changeItemList.add(new ChangeItem(ChangeType.DELETE, value, ChangeItemType.PHYSICAL_DB_GROUP));
        }

        return changeItemList;
    }

    private static void recycleOldBackendConnections(boolean forceAllReload, boolean closeFrontCon) {
        if (forceAllReload && closeFrontCon) {
            TraceManager.TraceObject traceObject = TraceManager.threadTrace("recycle-activeBackend-connections");
            ReloadLogHelper.briefInfo("recycle old active backend connections start");
            try {
                for (IOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                    for (BackendConnection con : processor.getBackends().values()) {
                        if (con.getPoolDestroyedTime() != 0) {
                            con.closeWithFront("old active backend conn will be forced closed by closing front conn");
                        }
                    }
                }
                ReloadLogHelper.briefInfo("recycle old active backend connections end");
            } finally {
                TraceManager.finishSpan(traceObject);
            }
        } else {
            ReloadLogHelper.briefInfo("skip recycle old active backend connections");
        }

    }


    private static void initFailed(Map<String, PhysicalDbGroup> newDbGroups) {
        // INIT FAILED
        ReloadLogHelper.briefInfo("reload failed, clear previously created dbInstances ");
        for (PhysicalDbGroup dbGroup : newDbGroups.values()) {
            dbGroup.stop("reload fail, stop");
        }
    }

    private static SystemVariables getSystemVariablesFromdbGroup(ConfigInitializer loader, Map<String, PhysicalDbGroup> newDbGroups) throws Exception {
        VarsExtractorHandler handler = new VarsExtractorHandler(newDbGroups);
        SystemVariables newSystemVariables;
        newSystemVariables = handler.execute();
        if (newSystemVariables == null) {
            if (loader.isFullyConfigured()) {
                throw new Exception("Can't get variables from any dbInstance, because all of dbGroup can't connect to MySQL correctly");
            } else {
                ReloadLogHelper.briefInfo("no valid dbGroup ,keep variables as old");
                newSystemVariables = DbleServer.getInstance().getSystemVariables();
            }
        }
        return newSystemVariables;
    }

    private static void recycleServerConnections() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("recycle-sharding-connections");
        ReloadLogHelper.briefInfo("recycle front connection start");
        try {
            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fcon : processor.getFrontends().values()) {
                    if (!fcon.isManager()) {
                        ReloadLogHelper.debug("recycle front connection:{}", fcon);
                        fcon.close("Reload causes the service to stop");
                    }
                }
            }
            ReloadLogHelper.briefInfo("recycle front connection end");
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static void writePacket(boolean isSuccess, ManagerService service, String errorMsg, int errorCode) {
        if (isSuccess) {
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage("Reload config success".getBytes());
            ok.write(service.getConnection());
        } else {
            service.writeErrMessage(errorCode, "Reload Failure, The reason is " + errorMsg);
        }
    }
}
