/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response.ha;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.values.HaInfo;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.PacketResult;
import com.actiontech.dble.singleton.HaConfigManager;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaEnable {

    private DbGroupHaEnable() {
    }

    public static void execute(Matcher enable, ManagerService service, PacketResult packetResult) {
        String dbGroupName = enable.group(1);
        String dbInstanceName = enable.group(3);
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dbGroup = DbleServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
            if (dbGroup == null) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("dbGroup " + dbGroupName + " do not exists");
                return;
            }


            if (!dbGroup.checkInstanceExist(dbInstanceName)) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Some of the dbInstanceName in command in " + dbGroup.getGroupName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, enable.group(0));
            if (ClusterConfig.getInstance().isClusterEnable()) {
                if (ClusterConfig.getInstance().isNeedSyncHa()) {
                    if (!enableWithCluster(id, dbGroup, dbInstanceName, service, packetResult)) {
                        return;
                    }
                } else {
                    try {
                        String result = dbGroup.enableHosts(dbInstanceName, true);
                        //only update for the status
                        ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dbGroup.getGroupName()), result);
                        // eg: all mysql instance is disabled when dble starts up, then enable a mysql instance
                        DbleServer.getInstance().pullVarAndMeta(dbGroup);
                    } catch (Exception e) {
                        HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("enable dataHost with error, use show @@dataSource to check latest status. Error:" + e.getMessage());
                        return;
                    }
                }
            } else {
                try {
                    String result = dbGroup.enableHosts(dbInstanceName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                    // eg: all mysql instance is disabled when dble starts up, then enable a mysql instance
                    DbleServer.getInstance().pullVarAndMeta(dbGroup);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("enable dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
                    return;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean enableWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerService mc, PacketResult packetResult) {
        //get the lock from ucore
        DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.ENABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );

        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is changing the dbGroup, please try again later.");
            return false;
        }
        try {
            String result = dh.enableHosts(subHostName, false);
            //only update for the status
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
            // eg: all mysql instance is disabled when dble starts up, then enable a mysql instance
            DbleServer.getInstance().pullVarAndMeta(dh);
        } catch (Exception e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
            return false;
        } finally {
            distributeLock.release();
        }
        return true;
    }


}
