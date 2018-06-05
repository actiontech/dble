/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil.SEPARATOR;


/**
 * @author mycat
 */
public final class RollbackConfig {
    private RollbackConfig() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackConfig.class);

    public static void execute(ManagerConnection c) {
        if (DbleServer.getInstance().isUseZK()) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            InterProcessMutex distributeLock = new InterProcessMutex(zkConn, KVPathUtil.getConfChangeLockPath());
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
                } else {
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
        } else if (DbleServer.getInstance().isUseUcore()) {
            UDistributeLock distributeLock = new UDistributeLock(UcorePathUtil.getConfChangeLockPath(),
                    UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
            try {
                if (!distributeLock.acquire()) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
                    return;
                }
                try {
                    rollbackWithUcore(c);
                    writeOKResult(c);
                } finally {
                    distributeLock.release();
                }
            } catch (Exception e) {
                LOGGER.info("reload config failure", e);
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            }
        } else {
            final ReentrantLock lock = DbleServer.getInstance().getConfig().getLock();
            lock.lock();
            try {
                rollback();
                writeOKResult(c);
            } catch (Exception e) {
                writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
            } finally {
                lock.unlock();
            }
        }
    }


    private static void rollbackWithUcore(ManagerConnection c) {
        //step 1 lock the local meta ,than all the query depends on meta will be hanging
        final ReentrantLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.lock();
        try {
            // step 2 rollback self config
            rollback();

            //step 3 tail the ucore & notify the other dble
            ConfStatus status = new ConfStatus(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ConfStatus.Status.ROLLBACK, null);
            ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getConfStatusPath(), status.toString());

            //step 4 set self status success
            ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), UcorePathUtil.SUCCESS);


            String errorMsg = ClusterUcoreSender.waitingForAllTheNode(UcorePathUtil.SUCCESS, UcorePathUtil.getConfStatusPath() + SEPARATOR);

            //step 6 delete the reload flag
            ClusterUcoreSender.deleteKVTree(UcorePathUtil.getConfStatusPath());

            if (errorMsg != null) {
                throw new RuntimeException(errorMsg);
            }
        } catch (Exception e) {
            LOGGER.warn("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.unlock();
        }
    }


    /**
     * start the rollback when the zk is in using
     *
     * @param zkConn
     * @param c
     */
    private static void rollbackWithZk(CuratorFramework zkConn, ManagerConnection c) {
        final ReentrantLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.lock();
        try {
            rollback();
            XmltoZkMain.rollbackConf();
            //tell zk this instance has prepared
            ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
            //check all session waiting status
            List<String> preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
            // TODO: While waiting, a new instance of MyCat is upping and working.
            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(KVPathUtil.getConfStatusPath());
            }
            for (String child : preparedList) {
                zkConn.delete().forPath(ZKPaths.makePath(KVPathUtil.getConfStatusPath(), child));
            }
            writeOKResult(c);
        } catch (Exception e) {
            LOGGER.info("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.unlock();
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

    public static void rollback() throws Exception {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        Map<String, PhysicalDBPool> dataHosts = conf.getBackupDataHosts();
        Map<String, UserConfig> users = conf.getBackupUsers();
        Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
        Map<String, PhysicalDBNode> dataNodes = conf.getBackupDataNodes();
        FirewallConfig firewall = conf.getBackupFirewall();
        Map<ERTable, Set<ERTable>> erRelations = conf.getBackupErRelations();
        boolean backDataHostWithoutWR = conf.backDataHostWithoutWR();
        if (conf.canRollback()) {
            conf.rollback(users, schemas, dataNodes, dataHosts, erRelations, firewall, backDataHostWithoutWR);
        } else if (conf.canRollbackAll()) {
            boolean rollbackStatus = true;
            String errorMsg = null;
            for (PhysicalDBPool dn : dataHosts.values()) {
                dn.init(dn.getActiveIndex());
                if (!dn.isInitSuccess()) {
                    rollbackStatus = false;
                    errorMsg = "dataHost[" + dn.getHostName() + "] inited failure";
                    break;
                }
            }
            // INIT FAILED
            if (!rollbackStatus) {
                for (PhysicalDBPool dn : dataHosts.values()) {
                    dn.clearDataSources("rollbackup config");
                    dn.stopHeartbeat();
                }
                throw new Exception(errorMsg);
            }
            final Map<String, PhysicalDBPool> cNodes = conf.getDataHosts();
            // apply
            conf.rollback(users, schemas, dataNodes, dataHosts, erRelations, firewall, backDataHostWithoutWR);
            // stop old resource heartbeat
            for (PhysicalDBPool dn : cNodes.values()) {
                dn.clearDataSources("clear old config ");
                dn.stopHeartbeat();
            }

        } else {
            throw new Exception("there is no old version");
        }
    }
}
