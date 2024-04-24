/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbGroupDiff;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.ConfigInitializer;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
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
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;
import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_COMMAND;

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

    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus, PacketResult packetResult) throws Exception {
        execute(service, loadAllMode, returnFlag, confStatus, packetResult, new ReloadContext());
    }

    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus, ReloadContext reloadContext) throws Exception {
        try {
            PacketResult packetResult = new PacketResult();
            if (ClusterConfig.getInstance().isClusterEnable()) {
                reloadWithCluster(service, loadAllMode, confStatus, packetResult, reloadContext);
            } else {
                reloadWithoutCluster(service, loadAllMode, returnFlag, confStatus, packetResult, reloadContext);
            }
            writePacket(packetResult.isSuccess(), service, packetResult.getErrorMsg(), packetResult.getErrorCode());
        } finally {
            ReloadManager.reloadFinish();
        }
    }

    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus, PacketResult packetResult, ReloadContext reloadContext) throws Exception {
        try {
            if (ClusterConfig.getInstance().isClusterEnable()) {
                reloadWithCluster(service, loadAllMode, confStatus, packetResult, reloadContext);
            } else {
                reloadWithoutCluster(service, loadAllMode, returnFlag, confStatus, packetResult, reloadContext);
            }
        } finally {
            ReloadManager.reloadFinish();
        }
    }

    private static void reloadWithCluster(ManagerService service, int loadAllMode, ConfStatus confStatus, PacketResult packetResult, ReloadContext reloadContext) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-with-cluster");
        try {
            DistributeLock distributeLock = null;
            if (!confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) && !confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) &&
                    !confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getConfChangeLockPath(), SystemConfig.getInstance().getInstanceName());
                if (!distributeLock.acquire()) {
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("Other instance is reloading, please try again later.");
                    packetResult.setErrorCode(ErrorCode.ER_YES);
                    return;
                }
                LOGGER.info("reload config: added distributeLock " + ClusterPathUtil.getConfChangeLockPath() + "");
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
                        reloadResult = reloadByConfig(loadAllMode, true, reloadContext);
                    } else {
                        reloadResult = reloadByLocalXml(loadAllMode, reloadContext);
                    }
                    if (!reloadResult.isSuccess()) {
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("Reload config failure.The reason is reload interruputed by others,config should be reload");
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
                    ClusterHelper.setKV(ClusterPathUtil.getConfStatusOperatorPath(), status.toString());
                    ReloadLogHelper.info("reload config: sent config status to cluster center", LOGGER);
                    //step 5 start a loop to check if all the dble in cluster is reload finished
                    ReloadManager.waitingOthers();
                    ClusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), ClusterPathUtil.SUCCESS);
                    final String errorMsg = ClusterLogic.waitingForAllTheNode(ClusterPathUtil.getConfStatusOperatorPath(), ClusterPathUtil.SUCCESS);
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
                        return;
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


    private static void reloadWithoutCluster(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus, PacketResult packetResult, ReloadContext reloadContext) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-in-local");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Reload config failure.The reason is reload status error ,other client or cluster may in reload");
                packetResult.setErrorCode(ErrorCode.ER_YES);
                return;
            }
            ReloadResult reloadResult;
            if (confStatus.getStatus().equals(ConfStatus.Status.MANAGER_INSERT) || confStatus.getStatus().equals(ConfStatus.Status.MANAGER_UPDATE) ||
                    confStatus.getStatus().equals(ConfStatus.Status.MANAGER_DELETE)) {
                reloadResult = reloadByConfig(loadAllMode, true, reloadContext);
            } else {
                reloadResult = reloadByLocalXml(loadAllMode, reloadContext);
            }
            if (reloadResult.isSuccess() && returnFlag) {
                // ok package
                return;
            } else if (!reloadResult.isSuccess()) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Reload config failure.The reason is reload interruputed by others,metadata should be reload");
                packetResult.setErrorCode(ErrorCode.ER_RELOAD_INTERRUPUTED);
            }
        } finally {
            lock.writeLock().unlock();
            TraceManager.finishSpan(service, traceObject);
        }
    }


    private static void writeErrorResult(ManagerService c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    @Deprecated
    public static ReloadResult reloadByLocalXml(final int loadAllMode, ReloadContext reloadContext) throws Exception {
        return reload(loadAllMode, null, null, null, null, reloadContext);
    }

    public static ReloadResult reloadByConfig(final int loadAllMode, boolean isWriteToLocal, ReloadContext reloadContext) throws Exception {
        String userConfig = DbleTempConfig.getInstance().getUserConfig();
        userConfig = StringUtil.isBlank(userConfig) ? DbleServer.getInstance().getConfig().getUserConfig() : userConfig;
        String dbConfig = DbleTempConfig.getInstance().getDbConfig();
        dbConfig = StringUtil.isBlank(dbConfig) ? DbleServer.getInstance().getConfig().getDbConfig() : dbConfig;
        String shardingConfig = DbleTempConfig.getInstance().getShardingConfig();
        shardingConfig = StringUtil.isBlank(shardingConfig) ? DbleServer.getInstance().getConfig().getShardingConfig() : shardingConfig;
        String sequenceConfig = DbleTempConfig.getInstance().getSequenceConfig();
        sequenceConfig = StringUtil.isBlank(sequenceConfig) ? DbleServer.getInstance().getConfig().getSequenceConfig() : sequenceConfig;
        ReloadResult reloadResult = reload(loadAllMode, userConfig, dbConfig, shardingConfig, sequenceConfig, reloadContext);
        DbleTempConfig.getInstance().clean();
        //sync json to local
        DbleServer.getInstance().getConfig().syncJsonToLocal(isWriteToLocal);
        return reloadResult;
    }

    private static ReloadResult reload(final int loadAllMode, String userConfig, String dbConfig, String shardingConfig, String sequenceConfig, ReloadContext reloadContext) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("self-reload");
        try {
            /*
             *  1 load new conf
             *  1.1 ConfigInitializer init adn check itself
             *  1.2 ShardingNode/dbGroup test connection
             */
            ReloadLogHelper.info("reload config: load all xml info start", LOGGER);
            ConfigInitializer loader;
            try {
                if (null == userConfig && null == dbConfig && null == shardingConfig && null == sequenceConfig) {
                    loader = new ConfigInitializer();
                } else {
                    loader = new ConfigInitializer(userConfig, dbConfig, shardingConfig, sequenceConfig);
                }
                loader.setReloadContext(reloadContext);
            } catch (Exception e) {
                throw new Exception(e);
            }
            ReloadLogHelper.info("reload config: load all xml info end", LOGGER);

            ReloadLogHelper.info("reload config: get variables from random alive dbGroup start", LOGGER);

            try {
                loader.testConnection();
            } catch (Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("just test ,not stop reload, catch exception", e);
                }
            }

            boolean forceAllReload = false;

            if ((loadAllMode & ManagerParseConfig.OPTR_MODE) != 0) {
                forceAllReload = true;
            }

            if (forceAllReload) {
                return forceReloadAll(loadAllMode, loader);
            } else {
                return intelligentReloadAll(loadAllMode, loader);
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static ReloadResult intelligentReloadAll(int loadAllMode, ConfigInitializer loader) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("self-intelligent-reload");
        try {
            /* 2.1.1 get diff of dbGroups */
            ServerConfig config = DbleServer.getInstance().getConfig();
            Map<String, PhysicalDbGroup> addOrChangeHosts = new LinkedHashMap<>();
            Map<String, PhysicalDbGroup> noChangeHosts = new LinkedHashMap<>();
            Map<String, PhysicalDbGroup> recycleHosts = new HashMap<>();
            distinguishDbGroup(loader.getDbGroups(), config.getDbGroups(), addOrChangeHosts, noChangeHosts, recycleHosts);

            Map<String, PhysicalDbGroup> mergedDbGroups = new LinkedHashMap<>();
            mergedDbGroups.putAll(noChangeHosts);
            mergedDbGroups.putAll(addOrChangeHosts);

            ConfigUtil.getAndSyncKeyVariables(mergedDbGroups, true);

            SystemVariables newSystemVariables = getSystemVariablesFromdbGroup(loader, mergedDbGroups);
            ReloadLogHelper.info("reload config: get variables from random node end", LOGGER);
            ServerConfig serverConfig = new ServerConfig(loader);

            if (loader.isFullyConfigured()) {
                if (newSystemVariables.isLowerCaseTableNames()) {
                    ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties start", LOGGER);
                    serverConfig.reviseLowerCase();
                    ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties end", LOGGER);
                }
                serverConfig.reloadSequence(loader.getSequenceConfig());
                serverConfig.selfChecking0();
            }
            checkTestConnIfNeed(loadAllMode, loader);

            Map<UserName, UserConfig> newUsers = serverConfig.getUsers();
            Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
            Map<String, ShardingNode> newShardingNodes = serverConfig.getShardingNodes();
            Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();
            Map<String, PhysicalDbGroup> newDbGroups = serverConfig.getDbGroups();
            Map<String, Properties> newBlacklistConfig = serverConfig.getBlacklistConfig();
            Map<String, AbstractPartitionAlgorithm> newFunctions = serverConfig.getFunctions();

            /*
             *  2 transform
             *  2.1 old lDbInstance continue to work
             *  2.1.1 define the diff of new & old dbGroup config
             *  2.1.2 create new init plan for the reload
             *  2.2 init the new lDbInstance
             *  2.3 transform
             *  2.4 put the old connection into a queue
             */


            /* 2.2 init the lDbInstance with diff*/
            ReloadLogHelper.info("reload config: init new dbGroup start", LOGGER);
            String reasonMsg = initDbGroupByMap(mergedDbGroups, newShardingNodes, loader.isFullyConfigured());
            ReloadLogHelper.info("reload config: init new dbGroup end", LOGGER);
            if (reasonMsg == null) {
                /* 2.3 apply new conf */
                ReloadLogHelper.info("reload config: apply new config start", LOGGER);
                boolean result;
                try {
                    result = config.reload(newUsers, newSchemas, newShardingNodes, mergedDbGroups, recycleHosts, newErRelations,
                            newSystemVariables, loader.isFullyConfigured(), loadAllMode, newBlacklistConfig, newFunctions,
                            loader.getUserConfig(), loader.getSequenceConfig(), loader.getShardingConfig(), loader.getDbConfig());
                    CronScheduler.getInstance().init(config.getSchemas());
                    if (!result) {
                        initFailed(newDbGroups);
                    }
                    FrontendUserManager.getInstance().initForLatest(newUsers, SystemConfig.getInstance().getMaxCon());
                    ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                    recycleOldBackendConnections((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0);
                    if (!loader.isFullyConfigured()) {
                        recycleServerConnections();
                    }
                    return new ReloadResult(result, addOrChangeHosts, recycleHosts);
                } catch (Exception e) {
                    initFailed(newDbGroups);
                    throw e;
                }
            } else {
                initFailed(newDbGroups);
                throw new Exception(reasonMsg);
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static void recycleOldBackendConnections(boolean closeFrontCon) {
        if (closeFrontCon) {
            for (IOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                for (BackendConnection con : processor.getBackends().values()) {
                    if (con.getPoolDestroyedTime() != 0) {
                        con.closeWithFront("old active backend conn will be forced closed by closing front conn");
                    }
                }
            }
        }
    }


    private static void initFailed(Map<String, PhysicalDbGroup> newDbGroups) {
        // INIT FAILED
        ReloadLogHelper.info("reload failed, clear previously created dbInstances ", LOGGER);
        for (PhysicalDbGroup dbGroup : newDbGroups.values()) {
            dbGroup.stop("reload fail, stop");
        }
    }

    private static ReloadResult forceReloadAll(final int loadAllMode, ConfigInitializer loader) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("self-force-reload");
        try {
            ServerConfig config = DbleServer.getInstance().getConfig();
            ServerConfig serverConfig = new ServerConfig(loader);
            Map<String, PhysicalDbGroup> newDbGroups = serverConfig.getDbGroups();

            ConfigUtil.getAndSyncKeyVariables(newDbGroups, true);

            SystemVariables newSystemVariables = getSystemVariablesFromdbGroup(loader, newDbGroups);
            ReloadLogHelper.info("reload config: get variables from random node end", LOGGER);
            // recycle old active conn
            if ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0) {
                for (IOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                    for (BackendConnection con : processor.getBackends().values()) {
                        if (con.getPoolDestroyedTime() != 0) {
                            con.close("old active backend conn will be forced closed by closing front conn");
                        }
                    }
                }
            }

            if (loader.isFullyConfigured()) {
                if (newSystemVariables.isLowerCaseTableNames()) {
                    ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties start", LOGGER);
                    serverConfig.reviseLowerCase();
                    ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties end", LOGGER);
                }
                serverConfig.reloadSequence(loader.getSequenceConfig());
                serverConfig.selfChecking0();
            }
            checkTestConnIfNeed(loadAllMode, loader);

            Map<UserName, UserConfig> newUsers = serverConfig.getUsers();
            Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
            Map<String, ShardingNode> newShardingNodes = serverConfig.getShardingNodes();
            Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();
            Map<String, Properties> newBlacklistConfig = serverConfig.getBlacklistConfig();
            Map<String, AbstractPartitionAlgorithm> newFunctions = serverConfig.getFunctions();


            ReloadLogHelper.info("reload config: init new dbGroup start", LOGGER);
            String reasonMsg = initDbGroupByMap(newDbGroups, newShardingNodes, loader.isFullyConfigured());
            ReloadLogHelper.info("reload config: init new dbGroup end", LOGGER);
            if (reasonMsg == null) {
                /* 2.3 apply new conf */
                ReloadLogHelper.info("reload config: apply new config start", LOGGER);
                boolean result;
                try {
                    Map<String, PhysicalDbGroup> oldDbGroupMap = config.getDbGroups();
                    result = config.reload(newUsers, newSchemas, newShardingNodes, newDbGroups, oldDbGroupMap, newErRelations,
                            newSystemVariables, loader.isFullyConfigured(), loadAllMode, newBlacklistConfig, newFunctions,
                            loader.getUserConfig(), loader.getSequenceConfig(), loader.getShardingConfig(), loader.getDbConfig());
                    CronScheduler.getInstance().init(config.getSchemas());
                    if (!result) {
                        initFailed(newDbGroups);
                    }
                    FrontendUserManager.getInstance().initForLatest(newUsers, SystemConfig.getInstance().getMaxCon());
                    ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                    if (!loader.isFullyConfigured()) {
                        recycleServerConnections();
                    }
                    return new ReloadResult(result, newDbGroups, oldDbGroupMap);
                } catch (Exception e) {
                    initFailed(newDbGroups);
                    throw e;
                }
            } else {
                initFailed(newDbGroups);
                throw new Exception(reasonMsg);
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static void checkTestConnIfNeed(int loadAllMode, ConfigInitializer loader) throws Exception {
        if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 && loader.isFullyConfigured()) {
            try {
                ReloadLogHelper.info("reload config: test all shardingNodes start", LOGGER);
                loader.testConnection();
                ReloadLogHelper.info("reload config: test all shardingNodes end", LOGGER);
            } catch (Exception e) {
                throw new Exception(e);
            }
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
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("recycle-sharding-connections");
        try {
            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fcon : processor.getFrontends().values()) {
                    if (!fcon.isManager()) {
                        fcon.close("Reload causes the service to stop");
                    }
                }
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static void distinguishDbGroup(Map<String, PhysicalDbGroup> newDbGroups, Map<String, PhysicalDbGroup> oldDbGroups,
                                           Map<String, PhysicalDbGroup> addOrChangeDbGroups, Map<String, PhysicalDbGroup> noChangeDbGroups,
                                           Map<String, PhysicalDbGroup> recycleHosts) {

        for (Map.Entry<String, PhysicalDbGroup> entry : newDbGroups.entrySet()) {
            PhysicalDbGroup oldPool = oldDbGroups.get(entry.getKey());
            PhysicalDbGroup newPool = entry.getValue();
            if (oldPool == null) {
                addOrChangeDbGroups.put(newPool.getGroupName(), newPool);
            } else {
                calcChangedDbGroups(addOrChangeDbGroups, noChangeDbGroups, recycleHosts, entry, oldPool);
            }
        }

        for (Map.Entry<String, PhysicalDbGroup> entry : oldDbGroups.entrySet()) {
            PhysicalDbGroup newPool = newDbGroups.get(entry.getKey());

            if (newPool == null) {
                PhysicalDbGroup oldPool = entry.getValue();
                recycleHosts.put(oldPool.getGroupName(), oldPool);
            }
        }
    }

    private static void calcChangedDbGroups(Map<String, PhysicalDbGroup> addOrChangeHosts, Map<String, PhysicalDbGroup> noChangeHosts, Map<String, PhysicalDbGroup> recycleHosts, Map.Entry<String, PhysicalDbGroup> entry, PhysicalDbGroup oldPool) {
        PhysicalDbGroupDiff toCheck = new PhysicalDbGroupDiff(entry.getValue(), oldPool);
        switch (toCheck.getChangeType()) {
            case PhysicalDbGroupDiff.CHANGE_TYPE_CHANGE:
                recycleHosts.put(toCheck.getNewPool().getGroupName(), toCheck.getOrgPool());
                addOrChangeHosts.put(toCheck.getNewPool().getGroupName(), toCheck.getNewPool());
                break;
            case PhysicalDbGroupDiff.CHANGE_TYPE_ADD:
                //when the type is change,just delete the old one and use the new one
                addOrChangeHosts.put(toCheck.getNewPool().getGroupName(), toCheck.getNewPool());
                break;
            case PhysicalDbGroupDiff.CHANGE_TYPE_NO:
                //add old dbGroup into the new mergeddbGroups
                noChangeHosts.put(toCheck.getNewPool().getGroupName(), toCheck.getOrgPool());
                break;
            case PhysicalDbGroupDiff.CHANGE_TYPE_DELETE:
                recycleHosts.put(toCheck.getOrgPool().getGroupName(), toCheck.getOrgPool());
                break;
            //do not add into old one
            default:
                break;
        }
    }


    private static String initDbGroupByMap(Map<String, PhysicalDbGroup> newDbGroups, Map<String, ShardingNode> newShardingNodes, boolean fullyConfigured) {
        String reasonMsg = null;
        for (PhysicalDbGroup dbGroup : newDbGroups.values()) {
            ReloadLogHelper.info("try to init dbGroup : " + dbGroup.toString(), LOGGER);
            String hostName = dbGroup.getGroupName();
            // set schemas
            ArrayList<String> dnSchemas = new ArrayList<>(30);
            for (ShardingNode dn : newShardingNodes.values()) {
                if (dn.getDbGroup().getGroupName().equals(hostName)) {
                    dn.setDbGroup(dbGroup);
                    dnSchemas.add(dn.getDatabase());
                }
            }
            dbGroup.setSchemas(dnSchemas.toArray(new String[dnSchemas.size()]));
            if (fullyConfigured) {
                dbGroup.init("reload config");
            } else {
                LOGGER.info("dbGroup[" + hostName + "] is not fullyConfigured, so doing nothing");
            }
        }
        return reasonMsg;
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

    public static class ReloadResult {
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
