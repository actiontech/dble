/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.*;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.xmltoKv.XmltoCluster;
import com.actiontech.dble.config.ConfigInitializer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.CronScheduler;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.config.loader.zkprocess.zktoxml.listen.ConfigStatusListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.ManagerParseConfig;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
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

        if (ClusterGeneralConfig.isUseZK()) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            InterProcessMutex distributeLock = new InterProcessMutex(zkConn, KVPathUtil.getConfChangeLockPath());
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rolling back, please try again later.");
                    return;
                }
                LOGGER.info("reload config: added distributeLock " + KVPathUtil.getConfChangeLockPath() + " to zk");
                ClusterDelayProvider.delayAfterReloadLock();
                try {
                    if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, ConfStatus.Status.RELOAD_ALL)) {
                        writeErrorResult(c, "Reload status error ,other client or cluster may in reload");
                        return;
                    }
                    reloadWithZookeeper(loadAllMode, zkConn, c);
                } finally {
                    distributeLock.release();
                    LOGGER.info("reload config: release distributeLock " + KVPathUtil.getConfChangeLockPath() + " from zk");
                }
            } catch (Exception e) {
                LOGGER.info("reload config using ZK failure", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getConfChangeLockPath(),
                    ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
            try {
                if (!distributeLock.acquire()) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rolling back, please try again later.");
                    return;
                }
                LOGGER.info("reload config: added distributeLock " + ClusterPathUtil.getConfChangeLockPath() + " to ucore");
                ClusterDelayProvider.delayAfterReloadLock();
                try {
                    if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, ConfStatus.Status.RELOAD_ALL)) {
                        writeErrorResult(c, "Reload status error ,other client or cluster may in reload");
                        return;
                    }
                    reloadWithUcore(loadAllMode, c);
                } finally {
                    distributeLock.release();
                    LOGGER.info("reload config: release distributeLock " + ClusterPathUtil.getConfChangeLockPath() + " from ucore");
                }
            } catch (Exception e) {
                LOGGER.info("reload config failure using ucore", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }

        } else {
            final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.writeLock().lock();
            try {
                try {
                    if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, ConfStatus.Status.RELOAD_ALL)) {
                        writeErrorResult(c, "Reload status error ,other client or cluster may in reload");
                        return;
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
        }

        ReloadManager.reloadFinish();
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
            ConfStatus status = new ConfStatus(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
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
            ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                    ConfigStatusListener.SUCCESS.getBytes(StandardCharsets.UTF_8));
            ReloadLogHelper.info("reload config: sent finished status to zk, waiting other instances", LOGGER);
            //check all session waiting status
            List<String> preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());

            ReloadManager.waitingOthers();
            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            }

            ReloadLogHelper.info("reload config: all instances finished ", LOGGER);
            ClusterDelayProvider.delayBeforeDeleteReloadLock();
            StringBuilder sbErrorInfo = new StringBuilder();
            for (String child : preparedList) {
                String childPath = ZKPaths.makePath(KVPathUtil.getConfStatusPath(), child);
                byte[] errorInfo = zkConn.getData().forPath(childPath);
                if (!ConfigStatusListener.SUCCESS.equals(new String(errorInfo, StandardCharsets.UTF_8))) {
                    sbErrorInfo.append(child).append(":");
                    sbErrorInfo.append(new String(errorInfo, StandardCharsets.UTF_8));
                    sbErrorInfo.append(";");
                }
                zkConn.delete().forPath(ZKPaths.makePath(KVPathUtil.getConfStatusPath(), child));
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
         *  1.2 DataNode/DataHost test connection
         */
        ReloadLogHelper.info("reload config: load all xml info start", LOGGER);
        ConfigInitializer loader;
        try {
            loader = new ConfigInitializer(false);
        } catch (Exception e) {
            throw new Exception(e);
        }
        ReloadLogHelper.info("reload config: load all xml info end", LOGGER);

        ReloadLogHelper.info("reload config: get variables from random alive data host start", LOGGER);

        try {
            loader.testConnection(false);
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
        /* 2.1.1 get diff of dataHosts */
        ServerConfig config = DbleServer.getInstance().getConfig();
        Map<String, AbstractPhysicalDBPool> addOrChangeHosts = new HashMap<>();
        Map<String, AbstractPhysicalDBPool> noChangeHosts = new HashMap<>();
        Map<String, AbstractPhysicalDBPool> recycleHosts = new HashMap<>();
        distinguishDataHost(loader.getDataHosts(), config.getDataHosts(), addOrChangeHosts, noChangeHosts, recycleHosts);

        Map<String, AbstractPhysicalDBPool> mergedDataHosts = new HashMap<>();
        mergedDataHosts.putAll(addOrChangeHosts);
        mergedDataHosts.putAll(noChangeHosts);

        SystemVariables newSystemVariables = getSystemVariablesFromDataHost(loader, mergedDataHosts);
        ReloadLogHelper.info("reload config: get variables from random node end", LOGGER);


        ServerConfig serverConfig = new ServerConfig(loader);
        if (newSystemVariables.isLowerCaseTableNames()) {
            ReloadLogHelper.info("reload config: data host's lowerCaseTableNames=1, lower the config properties start", LOGGER);
            serverConfig.reviseLowerCase();
            ReloadLogHelper.info("reload config: data host's lowerCaseTableNames=1, lower the config properties end", LOGGER);
        }
        checkTestConnIfNeed(loadAllMode, loader);
        ConfigUtil.getAndSyncKeyVariables(false, addOrChangeHosts);

        Map<String, UserConfig> newUsers = serverConfig.getUsers();
        Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
        Map<String, PhysicalDBNode> newDataNodes = serverConfig.getDataNodes();
        Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();
        FirewallConfig newFirewall = serverConfig.getFirewall();
        Map<String, AbstractPhysicalDBPool> newDataHosts = serverConfig.getDataHosts();

        /*
         *  2 transform
         *  2.1 old dataSource continue to work
         *  2.1.1 define the diff of new & old dataHost config
         *  2.1.2 create new init plan for the reload
         *  2.2 init the new dataSource
         *  2.3 transform
         *  2.4 put the old connection into a queue
         */


        /* 2.2 init the dataSource with diff*/
        ReloadLogHelper.info("reload config: init new data host  start", LOGGER);
        String reasonMsg = initDataHostByMap(mergedDataHosts, newDataNodes);
        ReloadLogHelper.info("reload config: init new data host end", LOGGER);
        if (reasonMsg == null) {
            /* 2.3 apply new conf */
            ReloadLogHelper.info("reload config: apply new config start", LOGGER);
            boolean result;
            try {
                result = config.reload(newUsers, newSchemas, newDataNodes, mergedDataHosts, addOrChangeHosts, recycleHosts, newErRelations, newFirewall,
                        newSystemVariables, loader.isDataHostWithoutWH(), loadAllMode);
                CronScheduler.getInstance().init(config.getSchemas());
                if (!result) {
                    initFailed(newDataHosts);
                }
                FrontendUserManager.getInstance().initForLatest(newUsers, loader.getSystem().getMaxCon());
                ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                recycleOldBackendConnections(recycleHosts, ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
                return result;
            } catch (Exception e) {
                initFailed(newDataHosts);
                throw e;
            }
        } else {
            initFailed(newDataHosts);
            throw new Exception(reasonMsg);
        }
    }

    private static void initFailed(Map<String, AbstractPhysicalDBPool> newDataHosts) throws Exception {
        // INIT FAILED
        ReloadLogHelper.info("reload failed, clear previously created data sources ", LOGGER);
        for (AbstractPhysicalDBPool dbPool : newDataHosts.values()) {
            dbPool.clearDataSources("reload config");
            dbPool.stopHeartbeat();
        }
    }

    private static boolean forceReloadAll(final int loadAllMode, ConfigInitializer loader) throws Exception {
        ServerConfig config = DbleServer.getInstance().getConfig();
        ServerConfig serverConfig = new ServerConfig(loader);
        Map<String, AbstractPhysicalDBPool> newDataHosts = serverConfig.getDataHosts();

        SystemVariables newSystemVariables = getSystemVariablesFromDataHost(loader, newDataHosts);
        ReloadLogHelper.info("reload config: get variables from random node end", LOGGER);

        if (newSystemVariables.isLowerCaseTableNames()) {
            ReloadLogHelper.info("reload config: data host's lowerCaseTableNames=1, lower the config properties start", LOGGER);
            serverConfig.reviseLowerCase();
            ReloadLogHelper.info("reload config: data host's lowerCaseTableNames=1, lower the config properties end", LOGGER);
        }
        checkTestConnIfNeed(loadAllMode, loader);
        ConfigUtil.getAndSyncKeyVariables(false, loader.getDataHosts());

        Map<String, UserConfig> newUsers = serverConfig.getUsers();
        Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
        Map<String, PhysicalDBNode> newDataNodes = serverConfig.getDataNodes();
        Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();
        FirewallConfig newFirewall = serverConfig.getFirewall();


        ReloadLogHelper.info("reload config: init new data host  start", LOGGER);
        String reasonMsg = initDataHostByMap(newDataHosts, newDataNodes);
        ReloadLogHelper.info("reload config: init new data host end", LOGGER);
        if (reasonMsg == null) {
            /* 2.3 apply new conf */
            ReloadLogHelper.info("reload config: apply new config start", LOGGER);
            boolean result;
            try {
                result = config.reload(newUsers, newSchemas, newDataNodes, newDataHosts, newDataHosts, config.getDataHosts(), newErRelations, newFirewall,
                        newSystemVariables, loader.isDataHostWithoutWH(), loadAllMode);
                CronScheduler.getInstance().init(config.getSchemas());
                if (!result) {
                    initFailed(newDataHosts);
                }
                FrontendUserManager.getInstance().initForLatest(newUsers, loader.getSystem().getMaxCon());
                ReloadLogHelper.info("reload config: apply new config end", LOGGER);
                recycleOldBackendConnections(config.getBackupDataHosts(), ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
                return result;
            } catch (Exception e) {
                initFailed(newDataHosts);
                throw e;
            }
        } else {
            initFailed(newDataHosts);
            throw new Exception(reasonMsg);
        }
    }

    private static void checkTestConnIfNeed(int loadAllMode, ConfigInitializer loader) throws Exception {
        if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0) {
            try {
                ReloadLogHelper.info("reload config: test all data Nodes start", LOGGER);
                loader.testConnection(false);
                ReloadLogHelper.info("reload config: test all data Nodes end", LOGGER);
            } catch (Exception e) {
                throw new Exception(e);
            }
        }
    }

    private static SystemVariables getSystemVariablesFromDataHost(ConfigInitializer loader, Map<String, AbstractPhysicalDBPool> newDataHosts) throws Exception {
        VarsExtractorHandler handler = new VarsExtractorHandler(newDataHosts);
        SystemVariables newSystemVariables;
        newSystemVariables = handler.execute();
        if (newSystemVariables == null) {
            if (!loader.isDataHostWithoutWH()) {
                throw new Exception("Can't get variables from any data host, because all of data host can't connect to MySQL correctly");
            } else {
                ReloadLogHelper.info("reload config: no valid data host ,keep variables as old", LOGGER);
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

    private static void recycleOldBackendConnections(Map<String, AbstractPhysicalDBPool> recycleMap, boolean closeFrontCon) {
        for (AbstractPhysicalDBPool dbPool : recycleMap.values()) {
            dbPool.stopHeartbeat();
            long oldTimestamp = System.currentTimeMillis();
            for (PhysicalDatasource ds : dbPool.getAllActiveDataSources()) {
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

    private static void distinguishDataHost(Map<String, AbstractPhysicalDBPool> newDataHosts, Map<String, AbstractPhysicalDBPool> oldDataHosts,
                                            Map<String, AbstractPhysicalDBPool> addOrChangeHosts, Map<String, AbstractPhysicalDBPool> noChangeHosts,
                                            Map<String, AbstractPhysicalDBPool> recycleHosts) {

        for (Map.Entry<String, AbstractPhysicalDBPool> entry : newDataHosts.entrySet()) {
            AbstractPhysicalDBPool oldPool = oldDataHosts.get(entry.getKey());
            AbstractPhysicalDBPool newPool = entry.getValue();
            if (oldPool == null) {
                addOrChangeHosts.put(newPool.getHostName(), newPool);
            } else {
                calcChangedDatahosts(addOrChangeHosts, noChangeHosts, recycleHosts, entry, oldPool);
            }
        }

        for (Map.Entry<String, AbstractPhysicalDBPool> entry : oldDataHosts.entrySet()) {
            AbstractPhysicalDBPool newPool = newDataHosts.get(entry.getKey());

            if (newPool == null) {
                AbstractPhysicalDBPool oldPool = entry.getValue();
                recycleHosts.put(oldPool.getHostName(), oldPool);
            }
        }
    }

    private static void calcChangedDatahosts(Map<String, AbstractPhysicalDBPool> addOrChangeHosts, Map<String, AbstractPhysicalDBPool> noChangeHosts, Map<String, AbstractPhysicalDBPool> recycleHosts, Map.Entry<String, AbstractPhysicalDBPool> entry, AbstractPhysicalDBPool oldPool) {
        PhysicalDBPoolDiff toCheck = new PhysicalDBPoolDiff(entry.getValue(), oldPool);
        switch (toCheck.getChangeType()) {
            case PhysicalDBPoolDiff.CHANGE_TYPE_CHANGE:
                recycleHosts.put(toCheck.getNewPool().getHostName(), toCheck.getOrgPool());
                addOrChangeHosts.put(toCheck.getNewPool().getHostName(), toCheck.getNewPool());
                break;
            case PhysicalDBPoolDiff.CHANGE_TYPE_ADD:
                //when the type is change,just delete the old one and use the new one
                addOrChangeHosts.put(toCheck.getNewPool().getHostName(), toCheck.getNewPool());
                break;
            case PhysicalDBPoolDiff.CHANGE_TYPE_NO:
                //add old dataHost into the new mergedDataHosts
                noChangeHosts.put(toCheck.getNewPool().getHostName(), toCheck.getOrgPool());
                break;
            case PhysicalDBPoolDiff.CHANGE_TYPE_DELETE:
                recycleHosts.put(toCheck.getOrgPool().getHostName(), toCheck.getOrgPool());
                break;
            //do not add into old one
            default:
                break;
        }
    }


    private static String initDataHostByMap(Map<String, AbstractPhysicalDBPool> newDataHosts, Map<String, PhysicalDBNode> newDataNodes) {
        String reasonMsg = null;
        for (AbstractPhysicalDBPool dbPool : newDataHosts.values()) {
            ReloadLogHelper.info("try to init dataSouce : " + dbPool.toString(), LOGGER);
            String hostName = dbPool.getHostName();
            // set schemas
            ArrayList<String> dnSchemas = new ArrayList<>(30);
            for (PhysicalDBNode dn : newDataNodes.values()) {
                if (dn.getDbPool().getHostName().equals(hostName)) {
                    dn.setDbPool(dbPool);
                    dnSchemas.add(dn.getDatabase());
                }
            }
            dbPool.setSchemas(dnSchemas.toArray(new String[dnSchemas.size()]));

            if (!dbPool.isInitSuccess()) {
                reasonMsg = "Init DbPool [" + dbPool.getHostName() + "] failed";
                break;
            }
        }
        return reasonMsg;
    }
}
