/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.general.ClusterGeneralDistributeLock;
import com.actiontech.dble.cluster.zkprocess.ZkDistributeLock;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.cluster.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.ConfigStatusListener;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
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
        if (ClusterConfig.getInstance().isClusterEnable()) {
            if (rollbackWithCluster(c)) return;
        } else {
            rollbackWithoutCluster(c);
        }
        ReloadManager.reloadFinish();
    }

    private static boolean rollbackWithCluster(ManagerConnection c) {
        DistributeLock distributeLock;
        if (ClusterConfig.getInstance().isUseZK()) {
            distributeLock = new ZkDistributeLock(ClusterPathUtil.getConfChangeLockPath(),
                    SystemConfig.getInstance().getInstanceName());
        } else {
            distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getConfChangeLockPath(),
                    SystemConfig.getInstance().getInstanceName());
        }
        try {
            if (!distributeLock.acquire()) {
                c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
                return true;
            }
            ClusterDelayProvider.delayAfterReloadLock();
            try {
                if (ClusterConfig.getInstance().isUseZK()) {
                    rollbackWithZk(ZKUtils.getConnection(), c);
                } else {
                    rollbackWithUcore(c);
                }
            } finally {
                distributeLock.release();
            }
        } catch (Exception e) {
            LOGGER.info("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        }
        return false;
    }

    private static void rollbackWithoutCluster(ManagerConnection c) {
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
            ConfStatus status = new ConfStatus(SystemConfig.getInstance().getInstanceName(), ConfStatus.Status.ROLLBACK, null);
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
            ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(),
                    ConfigStatusListener.SUCCESS.getBytes(StandardCharsets.UTF_8));
            //check all session waiting status
            List<String> preparedList = zkConn.getChildren().forPath(ClusterPathUtil.getConfStatusPath());
            List<String> onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());

            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(ClusterPathUtil.getConfStatusPath());
            }
            ReloadLogHelper.info("rollback config: all instances finished ", LOGGER);
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
        Map<String, PhysicalDbGroup> dbGroups = conf.getBackupDbGroups();
        Map<Pair<String, String>, UserConfig> users = conf.getBackupUsers();
        Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
        Map<String, ShardingNode> shardingNodes = conf.getBackupShardingNodes();
        Map<ERTable, Set<ERTable>> erRelations = conf.getBackupErRelations();
        boolean backIsFullyConfiged = conf.backIsFullyConfiged();
        if (conf.canRollbackAll()) {
            boolean rollbackStatus = true;
            String errorMsg = null;
            if (conf.isFullyConfigured()) {
                for (PhysicalDbGroup dn : dbGroups.values()) {
                    dn.init();
                    if (!dn.isInitSuccess()) {
                        rollbackStatus = false;
                        errorMsg = "dbGroup[" + dn.getGroupName() + "] inited failure";
                        break;
                    }
                }
                // INIT FAILED
                if (!rollbackStatus) {
                    for (PhysicalDbGroup dn : dbGroups.values()) {
                        dn.clearDbInstances("rollbackup config");
                        dn.stopHeartbeat();
                    }
                    throw new Exception(errorMsg);
                }
            }
            final Map<String, PhysicalDbGroup> cNodes = conf.getDbGroups();
            // apply
            boolean result = conf.rollback(users, schemas, shardingNodes, dbGroups, erRelations, backIsFullyConfiged);
            // stop old resource heartbeat
            for (PhysicalDbGroup dn : cNodes.values()) {
                dn.clearDbInstances("clear old config ");
                dn.stopHeartbeat();
            }
            if (!backIsFullyConfiged) {
                for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                    for (FrontendConnection fcon : processor.getFrontends().values()) {
                        if (fcon instanceof ServerConnection) {
                            ServerConnection scon = (ServerConnection) fcon;
                            scon.close("Reload causes the service to stop");
                        }
                    }
                }
            }
            return result;
        } else {
            throw new Exception("there is no old version");
        }
    }
}
