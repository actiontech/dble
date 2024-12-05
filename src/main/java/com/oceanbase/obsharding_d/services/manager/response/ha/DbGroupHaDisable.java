/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response.ha;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.FeedBackType;
import com.oceanbase.obsharding_d.cluster.values.HaInfo;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.services.manager.handler.PacketResult;
import com.oceanbase.obsharding_d.singleton.HaConfigManager;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaDisable {

    private DbGroupHaDisable() {

    }

    public static void execute(Matcher disable, PacketResult packetResult) {
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.HA);
        String dhName = disable.group(1);
        String subHostName = disable.group(3);
        //check the dbGroup is exists
        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            HaConfigManager.getInstance().info("added configLock");
            PhysicalDbGroup dh = OBsharding_DServer.getInstance().getConfig().getDbGroups().get(dhName);
            if (dh == null) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("dbGroup " + dhName + " do not exists");
                return;
            }

            if (!dh.checkInstanceExist(subHostName)) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, disable.group(0));
            if (ClusterConfig.getInstance().isClusterEnable()) {
                if (ClusterConfig.getInstance().isNeedSyncHa()) {
                    if (!disableWithCluster(id, dh, subHostName, packetResult)) {
                        return;
                    }
                } else {
                    try {
                        //local set disable
                        final RawJson result = dh.disableHosts(subHostName, true);
                        //update total dataSources status
                        clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(dh.getGroupName()), dh.getClusterHaJson());
                        HaConfigManager.getInstance().haFinish(id, null, result);
                    } catch (Exception e) {
                        HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("disable dataHost with error, use show @@dataSource to check latest status. Error:" + e.getMessage());
                        return;
                    }
                }
            } else {
                try {
                    //OBsharding-D start in single mode
                    RawJson result = dh.disableHosts(subHostName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("disable dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
                    return;
                }
            }
        } finally {
            lock.readLock().unlock();
            HaConfigManager.getInstance().info("released configLock");
        }
    }

    private static boolean disableWithCluster(int id, PhysicalDbGroup dh, String subHostName, PacketResult packetResult) {
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.HA);
        //get the lock from ucore
        DistributeLock distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.DISABLE,
                        HaInfo.HaStatus.INIT
                )
        );
        HaConfigManager.getInstance().info("added distributeLock, path: " + ClusterMetaUtil.getHaLockPath(dh.getGroupName()));
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is changing the dbGroup, please try again later.");
            HaConfigManager.getInstance().haFinish(id, "Other instance is changing the dbGroup, please try again later.", null);
            return false;
        }
        try {
            //local set disable
            final RawJson result = dh.disableHosts(subHostName, false);

            //update total dbInstance status
            clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(dh.getGroupName()), dh.getClusterHaJson());
            // update the notify value let other OBsharding-D to notify
            clusterHelper.setKV(ClusterMetaUtil.getHaResponseChildPath(dh.getGroupName()),
                    new HaInfo(dh.getGroupName(),
                            SystemConfig.getInstance().getInstanceName(),
                            HaInfo.HaType.DISABLE,
                            HaInfo.HaStatus.SUCCESS
                    ));
            //change log stage into wait others
            HaConfigManager.getInstance().haWaitingOthers(id);
            //waiting for other OBsharding-D to response
            clusterHelper.createSelfTempNode(ClusterPathUtil.getHaResponsePath(dh.getGroupName()), FeedBackType.SUCCESS);
            String errorMsg = ClusterLogic.forHA().waitingForAllTheNode(ClusterPathUtil.getHaResponsePath(dh.getGroupName()));
            //set  log stage to finish
            HaConfigManager.getInstance().haFinish(id, errorMsg, result);
            if (errorMsg != null) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg(errorMsg);
                return false;
            }
        } catch (Exception e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        } finally {
            ClusterHelper.cleanPath(ClusterPathUtil.getHaResponsePath(dh.getGroupName()));
            distributeLock.release();
            HaConfigManager.getInstance().info("released distributeLock");
        }
        return true;
    }

}
