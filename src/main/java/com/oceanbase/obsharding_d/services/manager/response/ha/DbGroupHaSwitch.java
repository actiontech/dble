/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response.ha;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
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
public final class DbGroupHaSwitch {

    private DbGroupHaSwitch() {
    }

    public static void execute(Matcher switcher, PacketResult packetResult) {
        String dbGroupName = switcher.group(1);
        String masterName = switcher.group(2);
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            HaConfigManager.getInstance().info("added configLock");
            PhysicalDbGroup dh = OBsharding_DServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
            if (dh == null) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("dbGroup " + dbGroupName + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, switcher.group(0));
            if (!dh.checkInstanceExist(masterName)) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                return;
            }
            if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().isNeedSyncHa()) {
                if (!switchWithCluster(id, dh, masterName, packetResult)) {
                    return;
                }
            } else {
                try {
                    //OBsharding-D start in single mode
                    RawJson result = dh.switchMaster(masterName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                } catch (Exception e) {
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("swtich dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    return;
                }
            }

        } finally {
            lock.readLock().unlock();
            HaConfigManager.getInstance().info("released configLock");
        }
    }

    private static boolean switchWithCluster(int id, PhysicalDbGroup dh, String subHostName, PacketResult packetResult) {
        //get the lock from ucore
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.HA);
        DistributeLock distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.SWITCH,
                        HaInfo.HaStatus.INIT
                )
        );
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is changing the dbGroup, please try again later.");
            return false;
        }
        HaConfigManager.getInstance().info("added distributeLock, path: " + ClusterMetaUtil.getHaLockPath(dh.getGroupName()));
        try {
            RawJson result = dh.switchMaster(subHostName, false);
            clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(dh.getGroupName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
        } catch (Exception e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        } finally {
            distributeLock.release();
            HaConfigManager.getInstance().info("released distributeLock");
        }
        return true;
    }
}
