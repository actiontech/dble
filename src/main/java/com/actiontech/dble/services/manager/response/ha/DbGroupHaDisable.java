/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response.ha;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.values.HaInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.HaConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaDisable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupHaDisable.class);

    private DbGroupHaDisable() {

    }

    public static void execute(Matcher disable, ManagerService service) {
        String dhName = disable.group(1);
        String subHostName = disable.group(3);

        //check the dbGroup is exists
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dh = DbleServer.getInstance().getConfig().getDbGroups().get(dhName);
            if (dh == null) {
                service.writeErrMessage(ErrorCode.ER_YES, "dbGroup " + dhName + " do not exists");
                return;
            }


            if (!dh.checkInstanceExist(subHostName)) {
                service.writeErrMessage(ErrorCode.ER_YES, "Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, disable.group(0));
            if (ClusterConfig.getInstance().isClusterEnable()) {
                if (ClusterConfig.getInstance().isNeedSyncHa()) {
                    if (!disableWithCluster(id, dh, subHostName, service)) {
                        return;
                    }
                } else {
                    try {
                        //local set disable
                        final String result = dh.disableHosts(subHostName, true);
                        //update total dataSources status
                        ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), dh.getClusterHaJson());
                        HaConfigManager.getInstance().haFinish(id, null, result);
                    } catch (Exception e) {
                        HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                        service.writeErrMessage(ErrorCode.ER_YES, "disable dataHost with error, use show @@dataSource to check latest status. Error:" + e.getMessage());
                        return;
                    }
                }
            } else {
                try {
                    //dble start in single mode
                    String result = dh.disableHosts(subHostName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    service.writeErrMessage(ErrorCode.ER_YES, "disable dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
                    return;
                }
            }

            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(service.getConnection());
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean disableWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerService mc) {
        //get the lock from ucore
        DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.DISABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        if (!distributeLock.acquire()) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dbGroup, please try again later.");
            HaConfigManager.getInstance().haFinish(id, "Other instance is changing the dbGroup, please try again later.", null);
            return false;
        }
        try {
            //local set disable
            final String result = dh.disableHosts(subHostName, false);

            //update total dbInstance status
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), dh.getClusterHaJson());
            // update the notify value let other dble to notify
            ClusterHelper.setKV(ClusterPathUtil.getHaResponsePath(dh.getGroupName()),
                    new HaInfo(dh.getGroupName(),
                            SystemConfig.getInstance().getInstanceName(),
                            HaInfo.HaType.DISABLE,
                            HaInfo.HaStatus.SUCCESS
                    ).toString());
            //change log stage into wait others
            HaConfigManager.getInstance().haWaitingOthers(id);
            //waiting for other dble to response
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getHaResponsePath(dh.getGroupName()), ClusterPathUtil.SUCCESS);
            String errorMsg = ClusterLogic.waitingForAllTheNode(ClusterPathUtil.getHaResponsePath(dh.getGroupName()), ClusterPathUtil.SUCCESS);
            //set  log stage to finish
            HaConfigManager.getInstance().haFinish(id, errorMsg, result);
            if (errorMsg != null) {
                mc.writeErrMessage(ErrorCode.ER_YES, errorMsg);
                return false;
            }
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        } finally {
            ClusterHelper.cleanPath(ClusterPathUtil.getHaResponsePath(dh.getGroupName()));
            distributeLock.release();
        }
        return true;
    }

}
