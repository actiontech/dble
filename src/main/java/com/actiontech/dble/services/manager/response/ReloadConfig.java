/*
 * Copyright (C) 2016-2020 ActionTech.
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
import com.actiontech.dble.singleton.CronScheduler;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
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


    public static void execute(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus) throws Exception {
        try {
            if (ClusterConfig.getInstance().isClusterEnable()) {
                reloadWithCluster(service, loadAllMode, returnFlag, confStatus);
            } else {
                reloadWithoutCluster(service, loadAllMode, returnFlag, confStatus);
            }
        } finally {
            ReloadManager.reloadFinish();
        }
    }


    private static void reloadWithCluster(ManagerService service, int loadAllMode, boolean returnFlag, ConfStatus confStatus) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-with-cluster");
        try {
            DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getConfChangeLockPath(), SystemConfig.getInstance().getInstanceName());
            if (!distributeLock.acquire()) {
                service.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading, please try again later.");
                return;
            }
            LOGGER.info("reload config: added distributeLock " + ClusterPathUtil.getConfChangeLockPath() + "");
            ClusterDelayProvider.delayAfterReloadLock();
            if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                writeErrorResult(service, "Reload status error ,other client or cluster may in reload");
                return;
            }
            //step 1 lock the local meta ,than all the query depends on meta will be hanging
            final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.writeLock().lock();
            try {
                //step 2 reload the local config file
                if (!reloadAll(loadAllMode)) {
                    writeSpecialError(service, "Reload interruputed by others,config should be reload");
                    return;
                }
                ReloadLogHelper.info("reload config: single instance(self) finished", LOGGER);
                ClusterDelayProvider.delayAfterMasterLoad();

                //step 3 if the reload with no error ,than write the config file into cluster center remote
                ClusterHelper.writeConfToCluster();
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
                    writeErrorResultForCluster(service, errorMsg);
                    return;
                }
                if (returnFlag) {
                    writeOKResult(service);
                }
            } finally {
                lock.writeLock().unlock();
                ClusterHelper.cleanPath(ClusterPathUtil.getConfStatusOperatorPath() + SEPARATOR);
                distributeLock.release();
            }
        } finally {
            TraceManager.finishSpan(service, traceObject);
        }
    }


    private static void reloadWithoutCluster(ManagerService service, final int loadAllMode, boolean returnFlag, ConfStatus confStatus) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "reload-in-local");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, confStatus)) {
                writeErrorResult(service, "Reload status error ,other client or cluster may in reload");
                return;
            }
            boolean reloadFlag = reloadAll(loadAllMode);
            if (reloadFlag && returnFlag) {
                writeOKResult(service);
            } else if (!reloadFlag) {
                writeSpecialError(service, "Reload interruputed by others,metadata should be reload");
            }
        } finally {
            lock.writeLock().unlock();
            TraceManager.finishSpan(service, traceObject);
        }
    }


    private static void writeOKResult(ManagerService service) {
        if (LOGGER.isInfoEnabled()) {
            ReloadLogHelper.info("send ok package to client " + String.valueOf(service), LOGGER);
        }

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Reload config success".getBytes());
        ok.write(service.getConnection());
    }

    private static void writeErrorResultForCluster(ManagerService service, String errorMsg) {
        String sb = "Reload config failed partially. The node(s) failed because of:[" + errorMsg + "]";
        LOGGER.warn(sb);
        if (errorMsg.contains("interrupt by command")) {
            service.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
        } else {
            service.writeErrMessage(ErrorCode.ER_CLUSTER_RELOAD, sb);
        }
    }

    private static void writeSpecialError(ManagerService service, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        service.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
    }

    private static void writeErrorResult(ManagerService c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static boolean reloadAll(final int loadAllMode) throws Exception {
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
                loader = new ConfigInitializer(false);
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

    private static boolean intelligentReloadAll(int loadAllMode, ConfigInitializer loader) throws Exception {
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
                } else {
                    serverConfig.loadSequence();
                    serverConfig.selfChecking0();
                }
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
                            newSystemVariables, loader.isFullyConfigured(), loadAllMode, newBlacklistConfig, newFunctions);
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
                    return result;
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

    private static boolean forceReloadAll(final int loadAllMode, ConfigInitializer loader) throws Exception {
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
                } else {
                    serverConfig.loadSequence();
                    serverConfig.selfChecking0();
                }
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
                    result = config.reload(newUsers, newSchemas, newShardingNodes, newDbGroups, config.getDbGroups(), newErRelations,
                            newSystemVariables, loader.isFullyConfigured(), loadAllMode, newBlacklistConfig, newFunctions);
                    CronScheduler.getInstance().init(config.getSchemas());
                    if (!result) {
                        initFailed(newDbGroups);
                    }
                    FrontendUserManager.getInstance().initForLatest(newUsers, SystemConfig.getInstance().getMaxCon());
                    ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                    if (!loader.isFullyConfigured()) {
                        recycleServerConnections();
                    }
                    return result;
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
}
