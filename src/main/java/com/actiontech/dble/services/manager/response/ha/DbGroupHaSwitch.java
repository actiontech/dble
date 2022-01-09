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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;

/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaSwitch {

    private DbGroupHaSwitch() {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupHaSwitch.class);

    public static void execute(Matcher switcher, ManagerService service, PacketResult packetResult) {
        String dbGroupName = switcher.group(1);
        String masterName = switcher.group(2);
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dh = DbleServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
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
                if (!switchWithCluster(id, dh, masterName, service, packetResult)) {
                    return;
                }
            } else {
                try {
                    //dble start in single mode
                    String result = dh.switchMaster(masterName, true);
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
        }
    }

    private static boolean switchWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerService mc, PacketResult packetResult) {
        //get the lock from ucore
        DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.SWITCH,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is changing the dbGroup, please try again later.");
            return false;
        }
        try {
            String result = dh.switchMaster(subHostName, false);
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
        } catch (Exception e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        } finally {
            distributeLock.release();
        }
        return true;
    }
}
