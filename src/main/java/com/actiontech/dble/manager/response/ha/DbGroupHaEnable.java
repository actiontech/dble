/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response.ha;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.general.ClusterGeneralDistributeLock;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaEnable {

    private DbGroupHaEnable() {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupHaEnable.class);

    public static void execute(Matcher enable, ManagerConnection mc) {
        String dbGroupName = enable.group(1);
        String dbInstanceName = enable.group(3);
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dbGroup = DbleServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
            if (dbGroup == null) {
                mc.writeErrMessage(ErrorCode.ER_YES, "dbGroup " + dbGroupName + " do not exists");
                return;
            }


            if (!dbGroup.checkInstanceExist(dbInstanceName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dbInstanceName in command in " + dbGroup.getGroupName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, enable.group(0));
            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                if (ClusterConfig.getInstance().isUseZK()) {
                    if (!enableWithZK(id, dbGroup, dbInstanceName, mc)) {
                        return;
                    }
                } else {
                    if (!enableWithCluster(id, dbGroup, dbInstanceName, mc)) {
                        return;
                    }
                }
            } else {
                try {
                    String result = dbGroup.enableHosts(dbInstanceName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    mc.writeErrMessage(ErrorCode.ER_YES, "enable dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
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


    public static boolean enableWithZK(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        InterProcessMutex distributeLock = new InterProcessMutex(zkConn, ClusterPathUtil.getHaLockPath(dh.getGroupName()));
        try {
            boolean locked = false;
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is change the dbGroup status");
                    return false;
                }
                locked = true;
                String result = dh.enableHosts(subHostName, false);
                DbGroupHaDisable.setStatusToZK(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), zkConn, result);
                HaConfigManager.getInstance().haFinish(id, null, result);
            } finally {
                if (locked) {
                    distributeLock.release();
                }
                LOGGER.info("reload config: release distributeLock " + ClusterPathUtil.getConfChangeLockPath() + " from zk");
            }
        } catch (Exception e) {
            LOGGER.info("reload config using ZK failure", e);
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        }
        return true;
    }


    public static boolean enableWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        ClusterGeneralDistributeLock distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.ENABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dbGroup, please try again later.");
                return false;
            }
            String result = dh.enableHosts(subHostName, false);
            //only update for the status
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            return false;
        } finally {
            distributeLock.release();
        }
        return true;
    }
}
