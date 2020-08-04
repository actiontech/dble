/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_COMMAND;


/**
 * @author mycat
 */
public final class RollbackConfig {
    private RollbackConfig() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackConfig.class);

    public static void execute(ManagerService service) {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            rollbackWithCluster(service);
        } else {
            rollbackWithoutCluster(service);
        }
        ReloadManager.reloadFinish();
    }

    private static void rollbackWithCluster(ManagerService c) {
        DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getConfChangeLockPath(),
                SystemConfig.getInstance().getInstanceName());
        if (!distributeLock.acquire()) {
            c.writeErrMessage(ErrorCode.ER_YES, "Other instance is reloading/rollbacking, please try again later.");
            return;
        }
        ClusterDelayProvider.delayAfterReloadLock();
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
            ClusterHelper.setKV(ClusterPathUtil.getConfStatusOperatorPath(), status.toString());
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getConfStatusOperatorPath(), ClusterPathUtil.SUCCESS);
            String errorMsg = ClusterLogic.waitingForAllTheNode(ClusterPathUtil.getConfStatusOperatorPath(), ClusterPathUtil.SUCCESS);

            ClusterDelayProvider.delayBeforeDeleterollbackLock();

            if (errorMsg != null) {
                writeErrorResultForCluster(c, errorMsg);
            } else {
                writeOKResult(c);
            }
        } catch (Exception e) {
            LOGGER.warn("reload config failure", e);
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            //step 6 delete the reload flag
            lock.writeLock().unlock();
            ClusterHelper.cleanPath(ClusterPathUtil.getConfStatusOperatorPath());
            distributeLock.release();
        }
    }

    private static void rollbackWithoutCluster(ManagerService c) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            if (!rollback(TRIGGER_TYPE_COMMAND)) {
                writeSpecialError(c, "Rollback interrupted by others,config should be reload");
            } else {
                writeOKResult(c);
            }
        } catch (Exception e) {
            writeErrorResult(c, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }


    private static void writeOKResult(ManagerService service) {
        LOGGER.info(String.valueOf(service) + "Rollback config success by manager");
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Rollback config success".getBytes());
        ok.write(service.getConnection());
    }

    private static void writeErrorResult(ManagerService c, String errorMsg) {
        String sb = "Rollback config failure.The reason is that " + errorMsg;
        LOGGER.info(sb + "." + c);
        c.writeErrMessage(ErrorCode.ER_YES, sb);
    }

    private static void writeSpecialError(ManagerService c, String errorMsg) {
        String sb = "Reload config failure.The reason is " + errorMsg;
        LOGGER.warn(sb);
        c.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
    }


    private static void writeErrorResultForCluster(ManagerService c, String errorMsg) {
        String sb = "Reload config failed partially. The node(s) failed because of:[" + errorMsg + "]";
        LOGGER.warn(sb);
        if (errorMsg.contains("interrupt by command")) {
            c.writeErrMessage(ErrorCode.ER_RELOAD_INTERRUPUTED, sb);
        } else {
            c.writeErrMessage(ErrorCode.ER_CLUSTER_RELOAD, sb);
        }
    }

    public static boolean rollback(String reloadType) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("rollback-local");
        try {
            if (!ReloadManager.startReload(reloadType, ConfStatus.Status.ROLLBACK)) {
                throw new Exception("Reload status error ,other client or cluster may in reload");
            }
            ServerConfig conf = DbleServer.getInstance().getConfig();
            Map<String, PhysicalDbGroup> dbGroups = conf.getBackupDbGroups();
            Map<UserName, UserConfig> users = conf.getBackupUsers();
            Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
            Map<String, ShardingNode> shardingNodes = conf.getBackupShardingNodes();
            Map<ERTable, Set<ERTable>> erRelations = conf.getBackupErRelations();
            boolean backIsFullyConfiged = conf.backIsFullyConfiged();
            if (conf.canRollbackAll()) {
                if (conf.isFullyConfigured()) {
                    for (PhysicalDbGroup dn : dbGroups.values()) {
                        dn.init("rollback up config");
                    }
                }
                final Map<String, PhysicalDbGroup> cNodes = conf.getDbGroups();
                // apply
                boolean result = conf.rollback(users, schemas, shardingNodes, dbGroups, erRelations, backIsFullyConfiged);
                // stop old resource heartbeat
                for (PhysicalDbGroup dn : cNodes.values()) {
                    dn.stop("initial failed, rollback up config");
                }
                if (!backIsFullyConfiged) {
                    for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                        for (FrontendConnection fcon : processor.getFrontends().values()) {
                            if (!fcon.isManager()) {
                                fcon.close("Reload causes the service to stop");
                            }
                        }
                    }
                }
                return result;
            } else {
                throw new Exception("there is no old version");
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }
}
