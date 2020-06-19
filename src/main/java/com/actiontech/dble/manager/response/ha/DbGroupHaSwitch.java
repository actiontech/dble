/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response.ha;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.general.ClusterGeneralDistributeLock;
import com.actiontech.dble.cluster.zkprocess.ZkDistributeLock;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
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

    public static void execute(Matcher switcher, ManagerConnection mc) {
        String dbGroupName = switcher.group(1);
        String masterName = switcher.group(2);
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dh = DbleServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
            if (dh == null) {
                mc.writeErrMessage(ErrorCode.ER_YES, "dbGroup " + dbGroupName + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, switcher.group(0));
            if (!dh.checkInstanceExist(masterName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                return;
            }
            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                if (ClusterConfig.getInstance().useZkMode()) {
                    if (!switchWithZK(id, dh, masterName, mc)) {
                        return;
                    }
                } else {
                    if (!switchWithCluster(id, dh, masterName, mc)) {
                        return;
                    }
                }
            } else {
                try {
                    //dble start in single mode
                    String result = dh.switchMaster(masterName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    mc.writeErrMessage(ErrorCode.ER_YES, "swtich dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
                    return;
                }
            }

            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(mc);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean switchWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        ClusterGeneralDistributeLock distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.SWITCH,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        boolean locked = false;
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dbGroup, please try again later.");
                return false;
            }
            locked = true;
            String result = dh.switchMaster(subHostName, false);
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        } finally {
            if (locked) {
                distributeLock.release();
            }
        }
        return true;
    }


    private static boolean switchWithZK(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        DistributeLock distributeLock = new ZkDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()), String.valueOf(System.currentTimeMillis()));

        if (!distributeLock.acquire()) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is change the dbGroup status");
            return false;
        }
        try {
            String result = dh.switchMaster(subHostName, false);
            DbGroupHaDisable.setStatusToZK(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), zkConn, result);
            HaConfigManager.getInstance().haFinish(id, null, result);

            return true;
        } catch (Exception e) {
            LOGGER.info("reload config using ZK failure", e);
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        } finally {
            distributeLock.release();
            LOGGER.info("reload config: release distributeLock " + ClusterPathUtil.getConfChangeLockPath() + " from zk");
        }
    }
}
