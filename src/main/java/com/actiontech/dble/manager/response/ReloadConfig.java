/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDBPoolDiff;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.xmltoKv.XmltoCluster;
import com.actiontech.dble.config.ConfigInitializer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.config.loader.zkprocess.zktoxml.listen.ConfigStatusListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.util.DnPropertyUtil;
import com.actiontech.dble.manager.ManagerConnection;
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
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;

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
                ReloadConfig.execute(c, false, 0);
                break;
            case ManagerParseConfig.CONFIG_ALL:
                ReloadConfig.execute(c, true, parser.getMode());
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

    private static void execute(ManagerConnection c, final boolean loadAll, final int loadAllMode) {

        if (DbleServer.getInstance().isUseZK()) {
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
                    reloadWithZookeeper(loadAll, loadAllMode, zkConn, c);
                } finally {
                    distributeLock.release();
                    LOGGER.info("reload config: release distributeLock " + KVPathUtil.getConfChangeLockPath() + " from zk");
                }
            } catch (Exception e) {
                LOGGER.info("reload config using ZK failure", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } else if (DbleServer.getInstance().isUseGeneralCluster()) {
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
                    reloadWithUcore(loadAll, loadAllMode, c);
                } finally {
                    distributeLock.release();
                    LOGGER.info("reload config: release distributeLock " + ClusterPathUtil.getConfChangeLockPath() + " from ucore");
                }
            } catch (Exception e) {
                LOGGER.info("reload config failure using ucore", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }

        } else {
            final ReentrantLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.lock();
            try {
                try {
                    load(loadAll, loadAllMode);
                    writeOKResult(c);
                } catch (Exception e) {
                    LOGGER.info("reload error", e);
                    writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * reload the config with ucore notify
     *
     * @param loadAll
     * @param loadAllMode
     * @param c
     */
    private static void reloadWithUcore(final boolean loadAll, final int loadAllMode, ManagerConnection c) {
        //step 1 lock the local meta ,than all the query depends on meta will be hanging
        final ReentrantLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.lock();
        try {
            //step 2 reload the local config file
            load(loadAll, loadAllMode);
            LOGGER.info("reload config: single instance(self) finished");
            ClusterDelayProvider.delayAfterMasterLoad();

            //step 3 if the reload with no error ,than write the config file into ucore remote
            XmltoCluster.initFileToUcore();
            LOGGER.info("reload config: sent config file to ucore");
            //step 4 write the reload flag and self reload result into ucore,notify the other dble to reload
            ConfStatus status = new ConfStatus(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                    loadAll ? ConfStatus.Status.RELOAD_ALL : ConfStatus.Status.RELOAD,
                    loadAll ? String.valueOf(loadAllMode) : null);
            ClusterHelper.setKV(ClusterPathUtil.getConfStatusPath(), status.toString());
            LOGGER.info("reload config: sent config status to ucore");
            ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), ClusterPathUtil.SUCCESS);
            LOGGER.info("reload config: sent finished status to ucore, waiting other instances");
            //step 5 start a loop to check if all the dble in cluster is reload finished

            final String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getConfStatusPath() + SEPARATOR);
            LOGGER.info("reload config: all instances finished ");
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
            lock.unlock();
        }
    }


    private static void reloadWithZookeeper(final boolean loadAll, final int loadAllMode, CuratorFramework zkConn, ManagerConnection c) {
        final ReentrantLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.lock();
        try {
            load(loadAll, loadAllMode);
            LOGGER.info("reload config: single instance(self) finished");
            ClusterDelayProvider.delayAfterMasterLoad();

            XmltoZkMain.writeConfFileToZK(loadAll, loadAllMode);
            LOGGER.info("reload config: sent config status to ucore");
            //tell zk this instance has prepared
            ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                    ConfigStatusListener.SUCCESS.getBytes(StandardCharsets.UTF_8));
            LOGGER.info("reload config: sent finished status to ucore, waiting other instances");
            //check all session waiting status
            List<String> preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
            // TODO: While waiting, a new instance of MyCat is upping and working.
            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            }
            LOGGER.info("reload config: all instances finished ");
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
            LOGGER.warn("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.unlock();
        }
    }


    private static void load(final boolean loadAll, final int loadAllMode) throws Exception {
        if (loadAll) {
            reloadAll(loadAllMode);
        } else {
            reload();
        }
    }

    private static void writeOKResult(ManagerConnection c) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("send ok package to client " + String.valueOf(c));
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
        c.writeErrMessage(ErrorCode.ER_CLUSTER_RELOAD, sb);
    }

    private static void writeErrorResult(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    public static void reloadAll(final int loadAllMode) throws Exception {
        /*
         *  1 load new conf
         *  1.1 ConfigInitializer init adn check itself
         *  1.2 DataNode/DataHost test connection
         */
        LOGGER.info("reload config: load all xml info start");
        ConfigInitializer loader;
        try {
            loader = new ConfigInitializer(true, false);
        } catch (Exception e) {
            throw new Exception(e);
        }
        LOGGER.info("reload config: load all xml info end");

        LOGGER.info("reload config: get variables from random alive data host start");
        SystemVariables newSystemVariables = null;
        try {
            loader.testConnection(false);
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("just test ,not stop reload, catch exception", e);
            }
        }

        /* 2.1.1 get diff of dataHosts */
        ServerConfig config = DbleServer.getInstance().getConfig();
        Set<PhysicalDBPoolDiff> dataHostDiffSet = distinguishDataHost(loader.getDataHosts(), config.getDataHosts());
        Map<String, PhysicalDBPool> recyclHost = new HashMap<String, PhysicalDBPool>();
        Map<String, PhysicalDBPool> mergedDataHosts = initDataHostMapWithMerge(dataHostDiffSet, recyclHost);

        VarsExtractorHandler handler = new VarsExtractorHandler(mergedDataHosts);
        newSystemVariables = handler.execute();
        if (newSystemVariables == null) {
            if (!loader.isDataHostWithoutWH()) {
                throw new Exception("Can't get variables from any data host, because all of data host can't connect to MySQL correctly");
            } else {
                LOGGER.info("reload config: no valid data host ,keep variables as old");
                newSystemVariables = DbleServer.getInstance().getSystemVariables();
            }
        }
        LOGGER.info("reload config: get variables from random node end");

        ServerConfig serverConfig = new ServerConfig(loader);
        if (newSystemVariables.isLowerCaseTableNames()) {
            LOGGER.info("reload config: data host's lowerCaseTableNames=1, lower the config properties start");
            serverConfig.reviseLowerCase();
            LOGGER.info("reload config: data host's lowerCaseTableNames=1, lower the config properties end");
        }
        Map<String, UserConfig> newUsers = serverConfig.getUsers();
        Map<String, SchemaConfig> newSchemas = serverConfig.getSchemas();
        Map<String, PhysicalDBNode> newDataNodes = serverConfig.getDataNodes();
        Map<ERTable, Set<ERTable>> newErRelations = serverConfig.getErRelations();
        FirewallConfig newFirewall = serverConfig.getFirewall();
        Map<String, PhysicalDBPool> newDataHosts = serverConfig.getDataHosts();

        if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0) {
            try {
                LOGGER.info("reload config: test all data Nodes start");
                loader.testConnection(false);
                LOGGER.info("reload config: test all data Nodes end");
            } catch (Exception e) {
                throw new Exception(e);
            }
        }

        /*
         *  2 transform
         *  2.1 old dataSource continue to work
         *  2.1.1 define the diff of new & old dataHost config
         *  2.1.2 create new init plan for the reload
         *  2.2 init the new dataSource
         *  2.3 transform
         *  2.4 put the old connection into a queue
         */


        String reasonMsg = null;


        /* 2.2 init the dataSource with diff*/


        LOGGER.info("reload config: init new data host  start");

        boolean mergeReload = true;

        if ((loadAllMode & ManagerParseConfig.OPTR_MODE) != 0) {
            mergeReload = false;
        }

        if (mergeReload) {
            reasonMsg = initDataHostByMap(mergedDataHosts, newDataNodes);
        } else {
            reasonMsg = initDataHostByMap(newDataHosts, newDataNodes);
        }
        LOGGER.info("reload config: init new data host  end");
        if (reasonMsg == null) {
            /* 2.3 apply new conf */
            LOGGER.info("reload config: apply new config start");
            if (mergeReload) {
                config.reload(newUsers, newSchemas, newDataNodes, mergedDataHosts, newErRelations, newFirewall,
                        newSystemVariables, loader.isDataHostWithoutWH(), true, loadAllMode);
                DbleServer.getInstance().getUserManager().initForLatest(newUsers, loader.getSystem().getMaxCon());
                LOGGER.info("reload config: apply new config end");
                recycleOldBackendConnections(recyclHost, ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
            } else {
                config.reload(newUsers, newSchemas, newDataNodes, newDataHosts, newErRelations, newFirewall,
                        newSystemVariables, loader.isDataHostWithoutWH(), true, loadAllMode);
                DbleServer.getInstance().getUserManager().initForLatest(newUsers, loader.getSystem().getMaxCon());
                LOGGER.info("reload config: apply new config end");
                recycleOldBackendConnections(config.getBackupDataHosts(), ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
            }

        } else {
            // INIT FAILED
            LOGGER.info("reload failed, clear previously created data sources ");
            for (PhysicalDBPool dbPool : newDataHosts.values()) {
                dbPool.clearDataSources("reload config");
                dbPool.stopHeartbeat();
            }
            throw new Exception(reasonMsg);
        }
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

    private static void recycleOldBackendConnections(Map<String, PhysicalDBPool> recycleMap, boolean closeFrontCon) {
        for (PhysicalDBPool dbPool : recycleMap.values()) {
            dbPool.stopHeartbeat();
            Long oldTimestamp = System.currentTimeMillis();
            for (PhysicalDatasource ds : dbPool.getAllDataSources()) {
                for (NIOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
                    for (BackendConnection con : processor.getBackends().values()) {
                        if (con instanceof MySQLConnection) {
                            MySQLConnection mysqlCon = (MySQLConnection) con;
                            LOGGER.info("mysqlCon.getPool() == " + mysqlCon.getPool() + " " + mysqlCon.getSchema() + " " + mysqlCon.getId());
                            if (mysqlCon.getPool() == ds) {
                                if (con.isBorrowed()) {
                                    if (closeFrontCon) {
                                        findAndcloseFrontCon(con);
                                    } else {
                                        con.setOldTimestamp(oldTimestamp);
                                        NIOProcessor.BACKENDS_OLD.add(con);
                                    }
                                } else {
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


    public static void reload() throws Exception {
        /* 1 load new conf, ConfigInitializer will check itself */
        ConfigInitializer loader;
        try {
            loader = new ConfigInitializer(false, DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
        } catch (Exception e) {
            throw new Exception(e);
        }

        ServerConfig serverConfig = new ServerConfig(loader);
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            serverConfig.reviseLowerCase();
        }

        Map<String, UserConfig> users = serverConfig.getUsers();
        Map<String, SchemaConfig> schemas = serverConfig.getSchemas();
        Map<String, PhysicalDBNode> dataNodes = serverConfig.getDataNodes();
        Map<String, PhysicalDBPool> dataHosts = serverConfig.getDataHosts();
        Map<ERTable, Set<ERTable>> erRelations = serverConfig.getErRelations();
        FirewallConfig firewall = serverConfig.getFirewall();



        /* 2 apply the new conf */
        DbleServer.getInstance().getConfig().reload(users, schemas, dataNodes, dataHosts, erRelations, firewall,
                DbleServer.getInstance().getSystemVariables(), loader.isDataHostWithoutWH(), false, 0);
        DbleServer.getInstance().getUserManager().initForLatest(users, loader.getSystem().getMaxCon());
    }


    private static Set<PhysicalDBPoolDiff> distinguishDataHost(Map<String, PhysicalDBPool> newDataHosts, Map<String, PhysicalDBPool> oldDataHosts) {
        Set<PhysicalDBPoolDiff> changeSet = new HashSet<>();

        for (Map.Entry<String, PhysicalDBPool> entry : newDataHosts.entrySet()) {
            PhysicalDBPool oldPoool = oldDataHosts.get(entry.getKey());
            if (oldPoool == null) {
                changeSet.add(new PhysicalDBPoolDiff(PhysicalDBPoolDiff.CHANGE_TYPE_ADD, entry.getValue(), null));
                continue;
            } else {
                changeSet.add(new PhysicalDBPoolDiff(entry.getValue(), oldPoool));
            }
        }

        for (Map.Entry<String, PhysicalDBPool> entry : oldDataHosts.entrySet()) {
            PhysicalDBPool newPoool = newDataHosts.get(entry.getKey());
            if (newPoool == null) {
                changeSet.add(new PhysicalDBPoolDiff(PhysicalDBPoolDiff.CHANGE_TYPE_DELETE, null, entry.getValue()));
            }
        }
        return changeSet;
    }


    private static Map<String, PhysicalDBPool> initDataHostMapWithMerge(Set<PhysicalDBPoolDiff> diffSet, Map<String, PhysicalDBPool> recyclHost) {
        Map<String, PhysicalDBPool> mergedDataHosts = new HashMap<String, PhysicalDBPool>();

        //make mergeDataHosts a Deep copy of oldDataHosts
        for (PhysicalDBPoolDiff diff : diffSet) {
            switch (diff.getChangeType()) {

                case PhysicalDBPoolDiff.CHANGE_TYPE_CHANGE:
                    recyclHost.put(diff.getNewPool().getHostName(), diff.getOrgPool());
                    mergedDataHosts.put(diff.getNewPool().getHostName(), diff.getNewPool());
                    break;
                case PhysicalDBPoolDiff.CHANGE_TYPE_ADD:
                    //when the type is change,just delete the old one and use the new one
                    mergedDataHosts.put(diff.getNewPool().getHostName(), diff.getNewPool());
                    break;
                case PhysicalDBPoolDiff.CHANGE_TYPE_NO:
                    //add old dataHost into the new mergedDataHosts
                    mergedDataHosts.put(diff.getNewPool().getHostName(), diff.getOrgPool());
                    break;
                case PhysicalDBPoolDiff.CHANGE_TYPE_DELETE:
                    recyclHost.put(diff.getOrgPool().getHostName(), diff.getOrgPool());
                    break;
                //do not add into old one
                default:
                    break;

            }
        }
        return mergedDataHosts;
    }


    private static String initDataHostByMap(Map<String, PhysicalDBPool> newDataHosts, Map<String, PhysicalDBNode> newDataNodes) {
        String reasonMsg = null;
        for (PhysicalDBPool dbPool : newDataHosts.values()) {
            LOGGER.info("try to into dataSouce : " + dbPool.toString());
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

            // get data host
            String dnIndex = DnPropertyUtil.loadDnIndexProps().getProperty(dbPool.getHostName(), "0");
            if (!"0".equals(dnIndex)) {
                LOGGER.info("init data host: " + dbPool.getHostName() + " to use datasource index:" + dnIndex);
            }

            dbPool.reloadInit(Integer.parseInt(dnIndex));
            if (!dbPool.isInitSuccess()) {
                reasonMsg = "Init DbPool [" + dbPool.getHostName() + "] failed";
                break;
            }
        }
        return reasonMsg;
    }
}
