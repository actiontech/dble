/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbGroupDiff;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.general.ClusterGeneralDistributeLock;
import com.actiontech.dble.cluster.general.xmltoKv.XmltoCluster;
import com.actiontech.dble.cluster.zkprocess.ZkDistributeLock;
import com.actiontech.dble.cluster.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.ConfigStatusListener;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.ConfigInitializer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.ManagerParseConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.singleton.CronScheduler;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;
import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_COMMAND;

/**
 * @author mycat
 * @author zhuam
 */
public final class ReloadConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadConfig.class);

    private ReloadConfig() {
    }

    public static void execute(ManagerConnection c, String stmt, int offset) {
        ManagerParseConfig parser = new ManagerParseConfig();
        int rs = parser.parse(stmt, offset);
        switch (rs) {
            case ManagerParseConfig.CONFIG:
            case ManagerParseConfig.CONFIG_ALL:
                ReloadConfig.execute(c, parser.getMode());
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

    private static void execute(ManagerConnection c, final int loadAllMode) {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            if (reloadWithCluster(c, loadAllMode)) return;
        } else {
            if (reloadWithoutCluster(c, loadAllMode)) return;
        }

        ReloadManager.reloadFinish();
    }

    private static boolean reloadWithCluster(ManagerConnection c, int loadAllMode) {
        DistributeLock distributeLock;
        if (ClusterConfig.getInstance().isUseZK()) {
            distributeLock = new ZkDistributeLock(ClusterPathUtil.getConfChangeLockPath(), SystemConfig.getInstance().getInstanceName());
        } else {
            distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getConfChangeLockPath(),
                    SystemConfig.getInstance().getInstanceName());
        }
        try {
            if (!distributeLock.acquire()) {
                c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rolling back, please try again later.");
                return true;
            }
            LOGGER.info("reload config: added distributeLock " + ClusterPathUtil.getConfChangeLockPath() + "");
            ClusterDelayProvider.delayAfterReloadLock();
            try {
                if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, ConfStatus.Status.RELOAD_ALL)) {
                    writeErrorResult(c, "Reload status error ,other client or cluster may in reload");
                    return true;
                }
                if (ClusterConfig.getInstance().isUseZK()) {
                    reloadWithZookeeper(loadAllMode, ZKUtils.getConnection(), c);
                } else {
                    reloadWithUcore(loadAllMode, c);
                }
            } finally {
                distributeLock.release();
                LOGGER.info("reload config: release distributeLock " + ClusterPathUtil.getConfChangeLockPath() + " from ucore");
            }
        } catch (Exception e) {
            LOGGER.info("reload config failure using ucore", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        }
        return false;
    }

    private static boolean reloadWithoutCluster(ManagerConnection c, int loadAllMode) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            try {
                if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, ConfStatus.Status.RELOAD_ALL)) {
                    writeErrorResult(c, "Reload status error ,other client or cluster may in reload");
                    return true;
                }
                if (reloadAll(loadAllMode)) {
                    writeOKResult(c);
                } else {
                    writeSpecialError(c, "Reload interruputed by others,metadata should be reload");
                }
            } catch (Exception e) {
                LOGGER.info("reload error", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

    private static void reloadWithUcore(final int loadAllMode, ManagerConnection c) {
        //step 1 lock the local meta ,than all the query depends on meta will be hanging
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            //step 2 reload the local config file
            if (!reloadAll(loadAllMode)) {
                writeSpecialError(c, "Reload interruputed by others,config should be reload");
                return;
            }
            ReloadLogHelper.info("reload config: single instance(self) finished", LOGGER);
            ClusterDelayProvider.delayAfterMasterLoad();

            ReloadManager.waitingOthers();
            //step 3 if the reload with no error ,than write the config file into ucore remote
            XmltoCluster.initFileToUcore();
            ReloadLogHelper.info("reload config: sent config file to ucore", LOGGER);
            //step 4 write the reload flag and self reload result into ucore,notify the other dble to reload
            ConfStatus status = new ConfStatus(SystemConfig.getInstance().getInstanceName(),
                    ConfStatus.Status.RELOAD_ALL, String.valueOf(loadAllMode));
            ClusterHelper.setKV(ClusterPathUtil.getConfStatusPath(), status.toString());
            ReloadLogHelper.info("reload config: sent config status to ucore", LOGGER);
            ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), ClusterPathUtil.SUCCESS);
            ReloadLogHelper.info("reload config: sent finished status to ucore, waiting other instances", LOGGER);
            //step 5 start a loop to check if all the dble in cluster is reload finished

            final String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getConfStatusPath() + SEPARATOR);
            ReloadLogHelper.info("reload config: all instances finished ", LOGGER);
            ClusterDelayProvider.delayBeforeDeleteReloadLock();
            //step 6 delete the reload flag
            ClusterHelper.cleanPath(ClusterPathUtil.getConfStatusPath() + SEPARATOR);

            if (errorMsg != null) {
                writeErrorResultForCluster(c, errorMsg);
                return;
            }
            writeOKResult(c);
        } catch (Exception e) {
            LOGGER.warn("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }


    private static void reloadWithZookeeper(final int loadAllMode, CuratorFramework zkConn, ManagerConnection c) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            if (!reloadAll(loadAllMode)) {
                writeSpecialError(c, "Reload interruputed by others,config should be reload");
                return;
            }
            ReloadLogHelper.info("reload config: single instance(self) finished", LOGGER);
            ClusterDelayProvider.delayAfterMasterLoad();

            XmltoZkMain.writeConfFileToZK(loadAllMode);
            ReloadLogHelper.info("reload config: sent config status to zk", LOGGER);
            //tell zk this instance has prepared
            ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(),
                    ConfigStatusListener.SUCCESS.getBytes(StandardCharsets.UTF_8));
            ReloadLogHelper.info("reload config: sent finished status to zk, waiting other instances", LOGGER);
            //check all session waiting status
            List<String> preparedList = zkConn.getChildren().forPath(ClusterPathUtil.getConfStatusPath());
            List<String> onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());

            ReloadManager.waitingOthers();
            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(ClusterPathUtil.getConfStatusPath());
            }

            ReloadLogHelper.info("reload config: all instances finished ", LOGGER);
            ClusterDelayProvider.delayBeforeDeleteReloadLock();
            StringBuilder sbErrorInfo = new StringBuilder();
            for (String child : preparedList) {
                String childPath = ZKPaths.makePath(ClusterPathUtil.getConfStatusPath(), child);
                byte[] errorInfo = zkConn.getData().forPath(childPath);
                if (!ConfigStatusListener.SUCCESS.equals(new String(errorInfo, StandardCharsets.UTF_8))) {
                    sbErrorInfo.append(child).append(":");
                    sbErrorInfo.append(new String(errorInfo, StandardCharsets.UTF_8));
                    sbErrorInfo.append(";");
                }
                zkConn.delete().forPath(ZKPaths.makePath(ClusterPathUtil.getConfStatusPath(), child));
            }

            if (sbErrorInfo.length() == 0) {
                writeOKResult(c);
            } else {
                writeErrorResultForCluster(c, sbErrorInfo.toString());
            }
        } catch (Exception e) {
            ReloadLogHelper.warn("reload config failure", e, LOGGER);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static void writeOKResult(ManagerConnection c) {
        if (LOGGER.isInfoEnabled()) {
            ReloadLogHelper.info("send ok package to client " + String.valueOf(c), LOGGER);
        }

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Reload config success".getBytes());
        ok.write(c);
    }

    private static void writeErrorResultForCluster(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failed partially. The node(s) failed because of:[" + errorMsg + "]";
        LOGGER.warn(sb);
        if (errorMsg.contains("interrupt by command")) {
            c.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
        } else {
            c.writeErrMessage(ErrorCode.ER_CLUSTER_RELOAD, sb);
        }
    }

    private static void writeSpecialError(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
    }

    private static void writeErrorResult(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static boolean reloadAll(final int loadAllMode) throws Exception {
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

    }

    private static boolean intelligentReloadAll(int loadAllMode, ConfigInitializer loader) throws Exception {
        /* 2.1.1 get diff of dbGroups */
        ServerConfig config = DbleServer.getInstance().getConfig();
        Map<String, PhysicalDbGroup> addOrChangeHosts = new HashMap<>();
        Map<String, PhysicalDbGroup> noChangeHosts = new HashMap<>();
        Map<String, PhysicalDbGroup> recycleHosts = new HashMap<>();
        distinguishDbGroup(loader.getDbGroups(), config.getDbGroups(), addOrChangeHosts, noChangeHosts, recycleHosts);

        Map<String, PhysicalDbGroup> mergedDbGroups = new HashMap<>();
        mergedDbGroups.putAll(addOrChangeHosts);
        mergedDbGroups.putAll(noChangeHosts);

        ConfigUtil.getAndSyncKeyVariables(mergedDbGroups, true);

        SystemVariables newSystemVariables = getSystemVariablesFromdbGroup(loader, mergedDbGroups);
        ReloadLogHelper.info("reload config: get variables from random node end", LOGGER);
        ServerConfig serverConfig = new ServerConfig(loader);

        if (newSystemVariables.isLowerCaseTableNames() && loader.isFullyConfigured()) {
            ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties start", LOGGER);
            serverConfig.reviseLowerCase();
            ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties end", LOGGER);
        }
        checkTestConnIfNeed(loadAllMode, loader);

        Map<Pair<String, String>, UserConfig> newUsers = serverConfig.getUsers();
        Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
        Map<String, ShardingNode> newShardingNodes = serverConfig.getShardingNodes();
        Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();
        Map<String, PhysicalDbGroup> newDbGroups = serverConfig.getDbGroups();

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
                result = config.reload(newUsers, newSchemas, newShardingNodes, mergedDbGroups, addOrChangeHosts, recycleHosts, newErRelations,
                        newSystemVariables, loader.isFullyConfigured(), loadAllMode);
                CronScheduler.getInstance().init(config.getSchemas());
                if (!result) {
                    initFailed(newDbGroups);
                }
                FrontendUserManager.getInstance().initForLatest(newUsers, SystemConfig.getInstance().getMaxCon());
                ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                recycleOldBackendConnections(recycleHosts, ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
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
    }

    private static void initFailed(Map<String, PhysicalDbGroup> newDbGroups) throws Exception {
        // INIT FAILED
        ReloadLogHelper.info("reload failed, clear previously created dbInstances ", LOGGER);
        for (PhysicalDbGroup dbGroup : newDbGroups.values()) {
            dbGroup.clearDbInstances("reload config");
            dbGroup.stopHeartbeat();
        }
    }

    private static boolean forceReloadAll(final int loadAllMode, ConfigInitializer loader) throws Exception {
        ServerConfig config = DbleServer.getInstance().getConfig();
        ServerConfig serverConfig = new ServerConfig(loader);
        Map<String, PhysicalDbGroup> newDbGroups = serverConfig.getDbGroups();

        ConfigUtil.getAndSyncKeyVariables(newDbGroups, true);

        SystemVariables newSystemVariables = getSystemVariablesFromdbGroup(loader, newDbGroups);
        ReloadLogHelper.info("reload config: get variables from random node end", LOGGER);

        if (newSystemVariables.isLowerCaseTableNames() && loader.isFullyConfigured()) {
            ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties start", LOGGER);
            serverConfig.reviseLowerCase();
            ReloadLogHelper.info("reload config: dbGroup's lowerCaseTableNames=1, lower the config properties end", LOGGER);
        }
        checkTestConnIfNeed(loadAllMode, loader);

        Map<Pair<String, String>, UserConfig> newUsers = serverConfig.getUsers();
        Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
        Map<String, ShardingNode> newShardingNodes = serverConfig.getShardingNodes();
        Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();


        ReloadLogHelper.info("reload config: init new dbGroup start", LOGGER);
        String reasonMsg = initDbGroupByMap(newDbGroups, newShardingNodes, loader.isFullyConfigured());
        ReloadLogHelper.info("reload config: init new dbGroup end", LOGGER);
        if (reasonMsg == null) {
            /* 2.3 apply new conf */
            ReloadLogHelper.info("reload config: apply new config start", LOGGER);
            boolean result;
            try {
                result = config.reload(newUsers, newSchemas, newShardingNodes, newDbGroups, newDbGroups, config.getDbGroups(), newErRelations,
                        newSystemVariables, loader.isFullyConfigured(), loadAllMode);
                CronScheduler.getInstance().init(config.getSchemas());
                if (!result) {
                    initFailed(newDbGroups);
                }
                FrontendUserManager.getInstance().initForLatest(newUsers, SystemConfig.getInstance().getMaxCon());
                ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                recycleOldBackendConnections(config.getBackupDbGroups(), ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
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

    private static void findAndcloseFrontCon(BackendConnection con) {
        if (con instanceof MySQLConnection) {
            MySQLConnection mcon1 = (MySQLConnection) con;
            for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fcon : processor.getFrontends().values()) {
                    if (fcon instanceof ServerConnection) {
                        ServerConnection scon = (ServerConnection) fcon;
                        Map<RouteResultsetNode, BackendConnection> bons = scon.getSession2().getTargetMap();
                        for (BackendConnection bcon : bons.values()) {
                            if (bcon instanceof MySQLConnection) {
                                MySQLConnection mcon2 = (MySQLConnection) bcon;
                                if (mcon1 == mcon2) {
                                    //frontEnd kill change to frontEnd close ,it's not necessary to use kill
                                    scon.close("reload config all");
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static void recycleServerConnections() {
        for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection fcon : processor.getFrontends().values()) {
                if (fcon instanceof ServerConnection) {
                    ServerConnection scon = (ServerConnection) fcon;
                    scon.close("Reload causes the service to stop");
                }
            }
        }
    }

    private static void recycleOldBackendConnections(Map<String, PhysicalDbGroup> recycleMap, boolean closeFrontCon) {
        for (PhysicalDbGroup dbGroup : recycleMap.values()) {
            dbGroup.stopHeartbeat();
            long oldTimestamp = System.currentTimeMillis();
            for (PhysicalDbInstance ds : dbGroup.getAllActiveDbInstances()) {
                for (NIOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                    for (BackendConnection con : processor.getBackends().values()) {
                        if (con instanceof MySQLConnection) {
                            MySQLConnection mysqlCon = (MySQLConnection) con;
                            if (mysqlCon.getPool() == ds) {
                                if (con.isBorrowed()) {
                                    if (closeFrontCon) {
                                        ReloadLogHelper.info("old active backend conn will be forced closed by closing front conn, conn info:" + mysqlCon, LOGGER);
                                        findAndcloseFrontCon(con);
                                    } else {
                                        ReloadLogHelper.info("old active backend conn will be added to old pool, conn info:" + mysqlCon, LOGGER);
                                        con.setOldTimestamp(oldTimestamp);
                                        NIOProcessor.BACKENDS_OLD.add(con);
                                    }
                                } else {
                                    ReloadLogHelper.info("old idle backend conn will be closed, conn info:" + mysqlCon, LOGGER);
                                    con.close("old idle conn for reload merge");
                                }
                            }
                        }
                    }
                }
            }
        }
        if (closeFrontCon) {
            for (NIOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                for (BackendConnection con : processor.getBackends().values()) {
                    if (con instanceof MySQLConnection) {
                        MySQLConnection mysqlCon = (MySQLConnection) con;
                        if (mysqlCon.getOldTimestamp() != 0) {
                            findAndcloseFrontCon(con);
                        }
                    }
                }
            }
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
            ReloadLogHelper.info("try to init dataSouce : " + dbGroup.toString(), LOGGER);
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
            if (!dbGroup.isInitSuccess() && fullyConfigured) {
                dbGroup.init();
                if (!dbGroup.isInitSuccess()) {
                    reasonMsg = "Init dbGroup [" + dbGroup.getGroupName() + "] failed";
                    break;
                }
            } else {
                LOGGER.info("dbGroup[" + hostName + "] already initiated, so doing nothing");
            }
        }
        return reasonMsg;
    }
}
