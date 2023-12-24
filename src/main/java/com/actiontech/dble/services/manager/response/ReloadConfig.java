/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
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
import com.actiontech.dble.config.util.ConfigUtil;
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
import com.actiontech.dble.services.manager.handler.PacketResult;
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
                    break;
                default:
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            }
        } catch (Exception e) {
            LOGGER.info("reload error", e);
            writeErrorResult(service, e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }


    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus) throws Exception {
        try {
            PacketResult packetResult = new PacketResult();
            if (ClusterConfig.getInstance().isClusterEnable()) {
                reloadWithCluster(service, loadAllMode, confStatus, packetResult);
            } else {
                reloadWithoutCluster(service, loadAllMode, returnFlag, confStatus, packetResult);
            }
            writePacket(packetResult.isSuccess(), service, packetResult.getErrorMsg(), packetResult.getErrorCode());
        } finally {
            ReloadManager.reloadFinish();
        }
    }

    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus, PacketResult packetResult) throws Exception {
        try {
            if (ClusterConfig.getInstance().isClusterEnable()) {
                reloadWithCluster(service, loadAllMode, confStatus, packetResult);
            } else {
                reloadWithoutCluster(service, loadAllMode, returnFlag, confStatus, packetResult);
            }
        } finally {
            ReloadManager.reloadFinish();
        }
    }

    private static void reloadWithCluster(ManagerService service, int loadAllMode, ConfStatus confStatus, PacketResult packetResult) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-with-cluster");
        try {

            ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
            DistributeLock distributeLock = null;
            if (!confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) && !confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) &&
                    !confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getConfChangeLockPath());
                if (!distributeLock.acquire()) {
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("Other instance is reloading, please try again later.");
                    packetResult.setErrorCode(ErrorCode.ER_YES);
                    return;
                }
                LOGGER.info("reload config: added distributeLock " + ClusterMetaUtil.getConfChangeLockPath() + "");
            }
            try {
                ClusterDelayProvider.delayAfterReloadLock();
                if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("Reload status error ,other client or cluster may in reload");
                    packetResult.setErrorCode(ErrorCode.ER_YES);
                    return;
                }
                //step 1 lock the local meta ,than all the query depends on meta will be hanging
                final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
                lock.writeLock().lock();
                try {
                    //step 2 reload the local config file
                    ReloadResult reloadResult;
                    if (confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) || confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) ||
                            confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                        reloadResult = reloadByConfig(loadAllMode, true);
                    } else {
                        reloadResult = reloadByLocalXml(loadAllMode);
                    }
                    if (!reloadResult.isSuccess()) {
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("Reload Failure.The reason is reload interruputed by others,config should be reload");
                        packetResult.setErrorCode(ErrorCode.ER_RELOAD_INTERRUPUTED);
                        return;
                    }
                    ReloadLogHelper.info("reload config: single instance(self) finished", LOGGER);
                    ClusterDelayProvider.delayAfterMasterLoad();

                    //step 3 if the reload with no error ,than write the config file into cluster center remote
                    ClusterHelper.writeConfToCluster(reloadResult);
                    ReloadLogHelper.info("reload config: sent config file to cluster center", LOGGER);

                    //step 4 write the reload flag and self reload result into cluster center,notify the other dble to reload
                    ConfStatus status = new ConfStatus(SystemConfig.getInstance().getInstanceName(),
                            ConfStatus.Status.RELOAD_ALL, String.valueOf(loadAllMode));
                    clusterHelper.setKV(ClusterMetaUtil.getConfStatusOperatorPath(), status);
                    ReloadLogHelper.info("reload config: sent config status to cluster center", LOGGER);
                    //step 5 start a loop to check if all the dble in cluster is reload finished
                    ReloadManager.waitingOthers();
                    clusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), FeedBackType.SUCCESS);
                    final String errorMsg = ClusterLogic.forConfig().waitingForAllTheNode(ClusterPathUtil.getConfStatusOperatorPath());
                    ReloadLogHelper.info("reload config: all instances finished ", LOGGER);
                    ClusterDelayProvider.delayBeforeDeleteReloadLock();

                    if (errorMsg != null) {
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("Reload config failed partially. The node(s) failed because of:[" + errorMsg + "]");
                        if (errorMsg.contains("interrupt by command")) {
                            packetResult.setErrorCode(ErrorCode.ER_RELOAD_INTERRUPUTED);
                        } else {
                            packetResult.setErrorCode(ErrorCode.ER_CLUSTER_RELOAD);
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                    ClusterHelper.cleanPath(ClusterPathUtil.getConfStatusOperatorPath() + SEPARATOR);
                }
            } finally {
                if (distributeLock != null) {
                    distributeLock.release();
                }
            }
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }
    }


    private static void reloadWithoutCluster(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus, PacketResult packetResult) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-in-local");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Reload Failure.The reason is reload status error ,other client or cluster may in reload");
                packetResult.setErrorCode(ErrorCode.ER_YES);
                return;
            }
            ReloadResult reloadResult;
            if (confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) || confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) ||
                    confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                reloadResult = reloadByConfig(loadAllMode, true);
            } else {
                reloadResult = reloadByLocalXml(loadAllMode);
            }
            if (reloadResult.isSuccess() && returnFlag) {
                // ok package
                return;
            } else if (!reloadResult.isSuccess()) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Reload Failure.The reason is reload interruputed by others,metadata should be reload");
                packetResult.setErrorCode(ErrorCode.ER_RELOAD_INTERRUPUTED);
            }
        } finally {
            lock.writeLock().unlock();
            TraceManager.finishSpan(service, traceObject);
        }
    }


    private static void writeErrorResult(ManagerService c, String errorMsg) {
        String sb = "Reload Failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static ReloadResult reloadByLocalXml(final int loadAllMode) throws Exception {
        return reload(loadAllMode, null, null, null, null);
    }

    public static ReloadResult reloadByConfig(final int loadAllMode, boolean isWriteToLocal) throws Exception {
        RawJson userConfig = DbleTempConfig.getInstance().getUserConfig();
        userConfig = userConfig == null ? DbleServer.getInstance().getConfig().getUserConfig() : userConfig;
        RawJson dbConfig = DbleTempConfig.getInstance().getDbConfig();
        dbConfig = dbConfig == null ? DbleServer.getInstance().getConfig().getDbConfig() : dbConfig;
        RawJson shardingConfig = DbleTempConfig.getInstance().getShardingConfig();
        shardingConfig = shardingConfig == null ? DbleServer.getInstance().getConfig().getShardingConfig() : shardingConfig;
        RawJson sequenceConfig = DbleTempConfig.getInstance().getSequenceConfig();
        sequenceConfig = sequenceConfig == null ? DbleServer.getInstance().getConfig().getSequenceConfig() : sequenceConfig;
        ReloadResult reloadResult = reload(loadAllMode, userConfig, dbConfig, shardingConfig, sequenceConfig);
        DbleTempConfig.getInstance().clean();
        //sync json to local
        DbleServer.getInstance().getConfig().syncJsonToLocal(isWriteToLocal);
        return reloadResult;
    }

    private static ReloadResult reload(final int loadAllMode, RawJson userConfig, RawJson dbConfig, RawJson shardingConfig, RawJson sequenceConfig) throws Exception {
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


            // lowerCase && load sequence
            if (loader.isFullyConfigured()) {
                if (newSystemVariables.isLowerCaseTableNames()) {
                    ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties start", LOGGER);
                    newConfig.reviseLowerCase();
                    ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties end", LOGGER);
                }
                newConfig.reloadSequence(loader.getSequenceConfig());
                newConfig.selfChecking0();
            }

            Map<UserName, UserConfig> newUsers = newConfig.getUsers();
            Map<String, SchemaConfig> newSchemas = newConfig.getSchemas();
            Map<String, ShardingNode> newShardingNodes = newConfig.getShardingNodes();
            Map<ERTable, Set<ERTable>> newErRelations = newConfig.getErRelations();
            Map<String, Set<ERTable>> newFuncNodeERMap = newConfig.getFuncNodeERMap();
            Map<String, Properties> newBlacklistConfig = newConfig.getBlacklistConfig();
            Map<String, AbstractPartitionAlgorithm> newFunctions = newConfig.getFunctions();


            // start/stop connection pool && heartbeat
            // replace config
            ReloadLogHelper.info("reload config: apply new config start", LOGGER);
            ServerConfig oldConfig = DbleServer.getInstance().getConfig();
            boolean result;
            try {
                result = oldConfig.reload(newUsers, newSchemas, newShardingNodes, newDbGroups, oldConfig.getDbGroups(), newErRelations, newFuncNodeERMap,
                        newSystemVariables, loader.isFullyConfigured(), loadAllMode, newBlacklistConfig, newFunctions,
                        loader.getUserConfig(), loader.getSequenceConfig(), loader.getShardingConfig(), loader.getDbConfig(), changeItemList);
                CronScheduler.getInstance().init(oldConfig.getSchemas());
                if (!result) {
                    initFailed(newDbGroups);
                }
                FrontendUserManager.getInstance().changeUser(changeItemList, SystemConfig.getInstance().getMaxCon());
                ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                // recycle old active conn
                recycleOldBackendConnections(!forceAllReload, (loadAllMode & ManagerParseConfig.OPTF_MODE) != 0);
                if (!loader.isFullyConfigured()) {
                    recycleServerConnections();
                }
                return packReloadResult(result, changeItemList, forceAllReload, newDbGroups, oldConfig.getDbGroups());
            } catch (Exception e) {
                initFailed(newDbGroups);
                throw e;
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static ReloadResult packReloadResult(boolean result, List<ChangeItem> changeItemList,
                                                 boolean forceAllReload,
                                                 Map<String, PhysicalDbGroup> newDbGroups,
                                                 Map<String, PhysicalDbGroup> oldDbGroups) {
        if (forceAllReload) {
            return new ReloadResult(result, newDbGroups, oldDbGroups);
        } else {
            Map<String, PhysicalDbGroup> addOrChangeMap0 = new HashMap<>();
            Map<String, PhysicalDbGroup> recycleMap0 = new HashMap<>();
            for (ChangeItem changeItem : changeItemList) {
                if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_GROUP) {
                    PhysicalDbGroup dbGroup = ((PhysicalDbGroup) changeItem.getItem());
                    switch (changeItem.getType()) {
                        case ADD:
                        case UPDATE:
                            addOrChangeMap0.put(dbGroup.getGroupName(), dbGroup);
                            break;
                        case DELETE:
                            recycleMap0.put(dbGroup.getGroupName(), dbGroup);
                            break;
                        default:
                            break;
                    }
                }
            }
            return new ReloadResult(result, addOrChangeMap0, recycleMap0);
        }
    }


    /**
     * check version/packetSize/lowerCase
     * get system variables
     */
    private static SystemVariables checkVersionAGetSystemVariables(ConfigInitializer loader, Map<String, PhysicalDbGroup> newDbGroups, List<ChangeItem> changeItemList, boolean forceAllReload) throws Exception {
        ReloadLogHelper.info("reload config: check and get system variables from random node start", LOGGER);
        SystemVariables newSystemVariables;
        if (forceAllReload || ConfigUtil.isAllDbInstancesChange(changeItemList)) {
            //check version/packetSize/lowerCase
            ConfigUtil.getAndSyncKeyVariables(newDbGroups, true);
            //system variables
            newSystemVariables = getSystemVariablesFromdbGroup(loader, newDbGroups);
        } else {
            //check version/packetSize/lowerCase
            ConfigUtil.getAndSyncKeyVariables(changeItemList, true);
            //system variables
            newSystemVariables = getSystemVariablesFromdbGroup(loader, loader.getDbGroups());
        }
        ReloadLogHelper.info("reload config: check and get system variables from random node end", LOGGER);
        return newSystemVariables;
    }

    /**
     * test connection
     */
    private static void testConnection(ConfigInitializer loader, List<ChangeItem> changeItemList, boolean forceAllReload, int loadAllMode) throws Exception {
        ReloadLogHelper.info("reload config: test connection start", LOGGER);
        try {
            //test connection
            if (forceAllReload && loader.isFullyConfigured()) {
                loader.testConnection();
            } else {
                syncShardingNode(loader);
                loader.testConnection(changeItemList);
            }
        } catch (Exception e) {
            if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 && loader.isFullyConfigured()) {
                //default/-f/-r
                throw new Exception(e);
            } else {
                //-s
                ReloadLogHelper.debug("just test ,not stop reload, catch exception", LOGGER, e);
            }
        }
        ReloadLogHelper.info("reload config: test connection end", LOGGER);
    }

    /**
     * compare change
     */
    private static List<ChangeItem> compareChange(ConfigInitializer loader) {
        ReloadLogHelper.info("reload config: compare changes start", LOGGER);
        List<ChangeItem> changeItemList = differentiateChanges(loader);
        ReloadLogHelper.debug("change items :{}", LOGGER, changeItemList);
        ReloadLogHelper.info("reload config: compare changes end", LOGGER);
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
                ReloadLogHelper.info("reload config: load config start [local xml]", LOGGER);
                loader = new ConfigInitializer();
            } else {
                ReloadLogHelper.info("reload config: load info start [memory]", LOGGER);
                ReloadLogHelper.debug("memory to Users is :{}\r\n" +
                        "memory to DbGroups is :{}\r\n" +
                        "memory to Shardings is :{}\r\n" +
                        "memory to sequence is :{}", LOGGER, userConfig, dbConfig, shardingConfig, sequenceConfig);
                loader = new ConfigInitializer(userConfig, dbConfig, shardingConfig, sequenceConfig);
            }
            ReloadLogHelper.info("reload config: load config end", LOGGER);
            return loader;
        } catch (Exception e) {
            throw new Exception(e.getMessage() == null ? e.toString() : e.getMessage(), e);
        }
    }

    private static void syncShardingNode(ConfigInitializer loader) {
        Map<String, ShardingNode> oldShardingNodeMap = DbleServer.getInstance().getConfig().getShardingNodes();
        Map<String, ShardingNode> newShardingNodeMap = loader.getShardingNodes();
        for (Map.Entry<String, ShardingNode> shardingNodeEntry : newShardingNodeMap.entrySet()) {
            //sync schema_exists,only testConn can update schema_exists
            if (oldShardingNodeMap.containsKey(shardingNodeEntry.getKey())) {
                shardingNodeEntry.getValue().setSchemaExists(oldShardingNodeMap.get(shardingNodeEntry.getKey()).isSchemaExists());
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
                    if (!newDbInstance.equalsForTestConn(oldDbInstance)) {
                        changeItem.setAffectTestConn(true);
                    } else {
                        newDbInstance.setTestConnSuccess(oldDbInstance.isTestConnSuccess());
                    }
                    return changeItem;
                }).forEach(changeItemList::add);
                //testConnSuccess with both
                for (Map.Entry<String, PhysicalDbInstance> dbInstanceEntry : dbInstanceMapDifference.entriesInCommon().entrySet()) {
                    dbInstanceEntry.getValue().setTestConnSuccess(oldDbInstanceMap.get(dbInstanceEntry.getKey()).isTestConnSuccess());
                }

            }
        }
        for (Map.Entry<String, PhysicalDbGroup> entry : removeDbGroup.entrySet()) {
            PhysicalDbGroup value = entry.getValue();
            changeItemList.add(new ChangeItem(ChangeType.DELETE, value, ChangeItemType.PHYSICAL_DB_GROUP));
        }

        return changeItemList;
    }

    private static PhysicalDbInstance getPhysicalDbInstance(ConfigInitializer loader) {
        PhysicalDbInstance ds = null;
        for (PhysicalDbGroup dbGroup : loader.getDbGroups().values()) {
            PhysicalDbInstance dsTest = dbGroup.getWriteDbInstance();
            if (dsTest.isTestConnSuccess()) {
                ds = dsTest;
            }
            if (ds != null) {
                break;
            }
        }
        if (ds == null) {
            for (PhysicalDbGroup dbGroup : loader.getDbGroups().values()) {
                for (PhysicalDbInstance dsTest : dbGroup.getDbInstances(false)) {
                    if (dsTest.isTestConnSuccess()) {
                        ds = dsTest;
                        break;
                    }
                }
                if (ds != null) {
                    break;
                }
            }
        }
        return ds;
    }

    private static void recycleOldBackendConnections(boolean forceAllReload, boolean closeFrontCon) {
        ReloadLogHelper.info("reload config: recycle old active backend [frontend] connections start", LOGGER);
        if (forceAllReload && closeFrontCon) {
            for (IOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                for (BackendConnection con : processor.getBackends().values()) {
                    if (con.getPoolDestroyedTime() != 0) {
                        con.closeWithFront("old active backend conn will be forced closed by closing front conn");
                    }
                }
            }
        }
        ReloadLogHelper.info("reload config: recycle old active backend [frontend] connections end", LOGGER);
    }


    private static void initFailed(Map<String, PhysicalDbGroup> newDbGroups) {
        // INIT FAILED
        ReloadLogHelper.info("reload failed, clear previously created dbInstances ", LOGGER);
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
                ReloadLogHelper.info("reload config: no valid dbGroup ,keep variables as old", LOGGER);
                newSystemVariables = DbleServer.getInstance().getSystemVariables();
            }
        }
        return newSystemVariables;
    }

    private static void recycleServerConnections() {
        ReloadLogHelper.info("reload config: recycle front connection start", LOGGER);
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("recycle-sharding-connections");
        try {
            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fcon : processor.getFrontends().values()) {
                    if (!fcon.isManager()) {
                        ReloadLogHelper.debug("recycle front connection:{}", LOGGER, fcon);
                        fcon.close("Reload causes the service to stop");
                    }
                }
            }
            ReloadLogHelper.info("reload config: recycle front connection end", LOGGER);
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static void writePacket(boolean isSuccess, ManagerService service, String errorMsg, int errorCode) {
        if (isSuccess) {
            if (LOGGER.isInfoEnabled()) {
                ReloadLogHelper.info("send ok package to client " + service, LOGGER);
            }
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage("Reload config success".getBytes());
            ok.write(service.getConnection());
        } else {
            LOGGER.warn(errorMsg);
            service.writeErrMessage(errorCode, errorMsg);
        }
    }


    public static class ReloadResult { // dbGroup
        private final boolean success;
        private final Map<String, PhysicalDbGroup> addOrChangeHostMap;
        private final Map<String, PhysicalDbGroup> recycleHostMap;

        public ReloadResult(boolean success, Map<String, PhysicalDbGroup> addOrChangeHostMap, Map<String, PhysicalDbGroup> recycleHostMap) {
            this.success = success;
            this.addOrChangeHostMap = addOrChangeHostMap;
            this.recycleHostMap = recycleHostMap;
        }

        public boolean isSuccess() {
            return success;
        }

        public Map<String, PhysicalDbGroup> getAddOrChangeHostMap() {
            return addOrChangeHostMap;
        }

        public Map<String, PhysicalDbGroup> getRecycleHostMap() {
            return recycleHostMap;
        }
    }
}
