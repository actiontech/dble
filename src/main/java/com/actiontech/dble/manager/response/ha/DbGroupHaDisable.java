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
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaDisable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupHaDisable.class);

    private DbGroupHaDisable() {

    }

    public static void execute(Matcher disable, ManagerConnection mc) {
        String dhName = disable.group(1);
        String subHostName = disable.group(3);

        //check the dbGroup is exists
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dh = DbleServer.getInstance().getConfig().getDbGroups().get(dhName);
            if (dh == null) {
                mc.writeErrMessage(ErrorCode.ER_YES, "dbGroup " + dhName + " do not exists");
                return;
            }


            if (!dh.checkInstanceExist(subHostName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, disable.group(0));
            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                if (ClusterConfig.getInstance().useZkMode()) {
                    if (!disableWithZK(id, dh, subHostName, mc)) {
                        return;
                    }
                } else {
                    if (!disableWithCluster(id, dh, subHostName, mc)) {
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
                    mc.writeErrMessage(ErrorCode.ER_YES, "disable dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
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

    private static boolean disableWithZK(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        DistributeLock distributeLock = new ZkDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()), String.valueOf(System.currentTimeMillis()));
        if (!distributeLock.acquire()) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Other dble instance is change the dbGroup status");
            HaConfigManager.getInstance().haFinish(id, "Other dble instance is changing the dbGroup, please try again later.", null);
            return false;
        }
        try {
            //local set disable
            final String result = dh.disableHosts(subHostName, false);
            // update total dbGroup status
            setStatusToZK(ClusterPathUtil.getHaStatusPath(dh.getGroupName()), zkConn, dh.getClusterHaJson());
            // write out notify message ,let other dble to response
            setStatusToZK(ClusterPathUtil.getHaResponsePath(dh.getGroupName()), zkConn, new HaInfo(dh.getGroupName(),
                    SystemConfig.getInstance().getInstanceName(),
                    HaInfo.HaType.DISABLE,
                    HaInfo.HaStatus.SUCCESS
            ).toString());
            //write out self change success result
            ZKUtils.createTempNode(ClusterPathUtil.getHaResponsePath(dh.getGroupName()), SystemConfig.getInstance().getInstanceName(),
                    ClusterPathUtil.SUCCESS.getBytes(StandardCharsets.UTF_8));
            //change stage into waiting others
            HaConfigManager.getInstance().haWaitingOthers(id);
            //use zk to waiting other dble to response
            String errorMessage = isZkFinished(zkConn, ClusterPathUtil.getHaResponsePath(dh.getGroupName()));

            //change stage into finished
            HaConfigManager.getInstance().haFinish(id, errorMessage, result);
            if (errorMessage != null && !"".equals(errorMessage)) {
                mc.writeErrMessage(ErrorCode.ER_YES, errorMessage);
                return false;
            }
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


    private static boolean disableWithCluster(int id, PhysicalDbGroup dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        ClusterGeneralDistributeLock distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.DISABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dbGroup, please try again later.");
                HaConfigManager.getInstance().haFinish(id, "Other instance is changing the dbGroup, please try again later.", null);
                return false;
            }
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
            //write out self success message
            ClusterHelper.setKV(ClusterPathUtil.getSelfResponsePath(ClusterPathUtil.getHaResponsePath(dh.getGroupName())), ClusterPathUtil.SUCCESS);
            //change log stage into wait others
            HaConfigManager.getInstance().haWaitingOthers(id);
            //waiting for other dble to response
            String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getHaResponsePath(dh.getGroupName()) + ClusterPathUtil.SEPARATOR);
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
            ClusterHelper.cleanPath(ClusterPathUtil.getHaResponsePath(dh.getGroupName()) + ClusterPathUtil.SEPARATOR);
            distributeLock.release();
        }
        return true;
    }


    static void setStatusToZK(String nodePath, CuratorFramework curator, String value) throws Exception {
        Stat stat = curator.checkExists().forPath(nodePath);
        if (null == stat) {
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), nodePath);
        }
        LOGGER.debug("ZkMultiLoader write file :" + nodePath + ", value :" + value);
        curator.setData().inBackground().forPath(nodePath, value.getBytes());
    }


    private static String isZkFinished(CuratorFramework zkConn, String preparePath) {
        try {
            List<String> preparedList = zkConn.getChildren().forPath(preparePath);
            List<String> onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());

            StringBuilder errorMsg = new StringBuilder();
            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());
                preparedList = zkConn.getChildren().forPath(preparePath);
            }
            for (String preparedNode : preparedList) {
                String nodePath = ZKPaths.makePath(preparePath, preparedNode);
                byte[] resultStatus = zkConn.getData().forPath(nodePath);
                String data = new String(resultStatus, StandardCharsets.UTF_8);
                if (!ClusterPathUtil.SUCCESS.equals(data)) {
                    errorMsg.append(preparedNode).append(":").append(data).append("\n");
                }
                zkConn.delete().forPath(ZKPaths.makePath(preparePath, preparedNode));
            }
            zkConn.delete().forPath(preparePath);
            return errorMsg.toString();
        } catch (Exception e) {
            LOGGER.warn("get error when waiting for others ", e);
            return e.getMessage();
        }
    }
}
