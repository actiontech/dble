/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.*;
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
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;
import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_COMMAND;


/**
 * @author mycat
 */
public final class RollbackConfig {
    private RollbackConfig() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackConfig.class);

    public static void execute(ManagerConnection c) {
        if (ClusterGeneralConfig.isUseZK()) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            InterProcessMutex distributeLock = new InterProcessMutex(zkConn, KVPathUtil.getConfChangeLockPath());
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
                } else {
                    ClusterDelayProvider.delayAfterReloadLock();
                    try {
                        rollbackWithZk(zkConn, c);
                    } finally {
                        distributeLock.release();
                    }
                }
            } catch (Exception e) {
                LOGGER.info("reload config failure", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getConfChangeLockPath(),
                    ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
            try {

                if (!distributeLock.acquire()) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
                    return;
                }
                ClusterDelayProvider.delayAfterReloadLock();
                try {
                    rollbackWithUcore(c);
                } finally {
                    distributeLock.release();
                }
            } catch (Exception e) {
                LOGGER.info("reload config failure", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } else {
            final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.writeLock().lock();
            try {
                if (!rollback(TRIGGER_TYPE_COMMAND)) {
                    writeSpecialError(c, "Rollback interruputed by others,config should be reload");
                } else {
                    writeOKResult(c);
                }
            } catch (Exception e) {
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            } finally {
                lock.writeLock().unlock();
            }
        }
        ReloadManager.reloadFinish();
    }


    private static void rollbackWithUcore(ManagerConnection c) {
        //step 1 lock the local meta ,than all the query depends on meta will be hanging
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            // step 2 rollback self config
            if (!rollback(TRIGGER_TYPE_COMMAND)) {
                writeSpecialError(c, "Rollback interruputed by others,config should be reload");
                return;
            }

            ReloadManager.waitingOthers();
            ClusterDelayProvider.delayAfterMasterRollback();

            //step 3 tail the ucore & notify the other dble
            ConfStatus status = new ConfStatus(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ConfStatus.Status.ROLLBACK, null);
            ClusterHelper.setKV(ClusterPathUtil.getConfStatusPath(), status.toString());

            //step 4 set self status success
            ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), ClusterPathUtil.SUCCESS);


            String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getConfStatusPath() + SEPARATOR);

            ClusterDelayProvider.delayBeforeDeleterollbackLock();
            //step 6 delete the reload flag
            ClusterHelper.cleanPath(ClusterPathUtil.getConfStatusPath());

            if (errorMsg != null) {
                writeErrorResultForCluster(c, errorMsg);
            } else {
                writeOKResult(c);
            }
        } catch (Exception e) {
            LOGGER.warn("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }


    private static void rollbackWithZk(CuratorFramework zkConn, ManagerConnection c) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            if (!rollback(TRIGGER_TYPE_COMMAND)) {
                writeSpecialError(c, "Rollback interruputed by others,config should be reload");
                return;
            }

            ReloadManager.waitingOthers();
            ClusterDelayProvider.delayAfterMasterRollback();

            XmltoZkMain.rollbackConf();
            //tell zk this instance has prepared
            ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                    ConfigStatusListener.SUCCESS.getBytes(StandardCharsets.UTF_8));
            //check all session waiting status
            List<String> preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());

            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            }
            ReloadLogHelper.info("rollback config: all instances finished ", LOGGER);
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
            LOGGER.info("rollback config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }


    private static void writeOKResult(ManagerConnection c) {
        LOGGER.info(String.valueOf(c) + "Rollback config success by manager");
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Rollback config success".getBytes());
        ok.write(c);
    }

    private static void writeErrorResult(ManagerConnection c, String errorMsg) {
        String sb = "Rollback config failure.The reason is that " + errorMsg;
        LOGGER.info(sb + "." + String.valueOf(c));
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    private static void writeSpecialError(ManagerConnection c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
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

    public static boolean rollback(String reloadType) throws Exception {
        if (!ReloadManager.startReload(reloadType, ConfStatus.Status.ROLLBACK)) {
            throw new Exception("Reload status error ,other client or cluster may in reload");
        }
        ServerConfig conf = DbleServer.getInstance().getConfig();
        Map<String, AbstractPhysicalDBPool> dataHosts = conf.getBackupDataHosts();
        Map<String, UserConfig> users = conf.getBackupUsers();
        Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
        Map<String, PhysicalDBNode> dataNodes = conf.getBackupDataNodes();
        FirewallConfig firewall = conf.getBackupFirewall();
        Map<ERTable, Set<ERTable>> erRelations = conf.getBackupErRelations();
        boolean backDataHostWithoutWR = conf.backDataHostWithoutWR();
        if (conf.canRollbackAll()) {
            boolean rollbackStatus = true;
            String errorMsg = null;
            for (AbstractPhysicalDBPool dn : dataHosts.values()) {
                dn.init();
                if (!dn.isInitSuccess()) {
                    rollbackStatus = false;
                    errorMsg = "dataHost[" + dn.getHostName() + "] inited failure";
                    break;
                }
            }
            // INIT FAILED
            if (!rollbackStatus) {
                for (AbstractPhysicalDBPool dn : dataHosts.values()) {
                    dn.clearDataSources("rollbackup config");
                    dn.stopHeartbeat();
                }
                throw new Exception(errorMsg);
            }
            final Map<String, AbstractPhysicalDBPool> cNodes = conf.getDataHosts();
            // apply
            boolean result = conf.rollback(users, schemas, dataNodes, dataHosts, erRelations, firewall, backDataHostWithoutWR);
            // stop old resource heartbeat
            for (AbstractPhysicalDBPool dn : cNodes.values()) {
                dn.clearDataSources("clear old config ");
                dn.stopHeartbeat();
            }
            return result;
        } else {
            throw new Exception("there is no old version");
        }
    }
}
