package com.actiontech.dble.manager.response;

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
public final class DataHostSwitch {

    private DataHostSwitch() {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DataHostSwitch.class);

    public static void execute(Matcher switcher, ManagerConnection mc) {
        String dhName = switcher.group(1);
        String masterName = switcher.group(2);
        boolean useCluster = ClusterConfig.getInstance().isNeedSyncHa();
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dh = DbleServer.getInstance().getConfig().getDbGroups().get(dhName);
            if (dh == null) {
                mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, switcher.group(0));
            if (!dh.checkDataSourceExist(masterName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dataSource in command in " + dh.getGroupName() + " do not exists");
                return;
            }
            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                if (ClusterConfig.getInstance().isUseZK()) {
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
                    mc.writeErrMessage(ErrorCode.ER_YES, "swtich dataHost with error, use show @@dataSource to check latest status. Error:" + e.getMessage());
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

    public static boolean switchWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        ClusterGeneralDistributeLock distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceId(),
                        HaInfo.HaType.DATAHOST_SWITCH,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        boolean locked = false;
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dataHost, please try again later.");
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


    public static boolean switchWithZK(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        InterProcessMutex distributeLock = new InterProcessMutex(zkConn, ClusterPathUtil.getHaLockPath(dh.getGroupName()));
        try {
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is change the dataHost status");
                    return false;
                }
                String result = dh.switchMaster(subHostName, false);
                DataHostDisable.setStatusToZK(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), zkConn, result);
                HaConfigManager.getInstance().haFinish(id, null, result);
            } finally {
                distributeLock.release();
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
}
