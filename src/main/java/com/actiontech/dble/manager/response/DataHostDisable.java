package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
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

import static com.actiontech.dble.util.KVPathUtil.SEPARATOR;

/**
 * Created by szf on 2019/10/22.
 */
public final class DataHostDisable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataHostDisable.class);

    private DataHostDisable() {

    }

    public static void execute(Matcher disable, ManagerConnection mc) {
        String dhName = disable.group(1);
        String subHostName = disable.group(3);
        boolean useCluster = ClusterHelper.useClusterHa();

        //check the dataHost is exists
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDataHost dh = DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
            if (dh == null) {
                mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
                return;
            }


            if (!dh.checkDataSourceExist(subHostName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dataSource in command in " + dh.getHostName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, disable.group(0));
            if (ClusterGeneralConfig.isUseGeneralCluster() && useCluster) {
                if (!disableWithCluster(id, dh, subHostName, mc)) {
                    return;
                }
            } else if (ClusterGeneralConfig.isUseZK() && useCluster) {
                if (!disableWithZK(id, dh, subHostName, mc)) {
                    return;
                }
            } else {
                try {
                    //dble start in single mode
                    String result = dh.disableHosts(subHostName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    mc.writeErrMessage(ErrorCode.ER_YES, "disable dataHost with error, use show @@dataSource to check latest status. Error:" + e.getMessage());
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

    public static boolean disableWithZK(int id, PhysicalDataHost dh, String subHostName, ManagerConnection mc) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        InterProcessMutex distributeLock = new InterProcessMutex(zkConn, KVPathUtil.getHaLockPath(dh.getHostName()));
        try {
            boolean locked = false;
            try {
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is change the dataHost status");
                    HaConfigManager.getInstance().haFinish(id, "Other instance is changing the dataHost, please try again later.", null);
                    return false;
                }
                locked = true;
                //local set disable
                final String result = dh.disableHosts(subHostName, false);
                // update total dataHost status
                setStatusToZK(KVPathUtil.getHaStatusPath(dh.getHostName()), zkConn, dh.getClusterHaJson());
                // write out notify message ,let other dble to response
                setStatusToZK(KVPathUtil.getHaResponsePath(dh.getHostName()), zkConn, new HaInfo(dh.getHostName(),
                        ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                        HaInfo.HaType.DATAHOST_DISABLE,
                        HaInfo.HaStatus.SUCCESS
                ).toString());
                //write out self change success result
                ZKUtils.createTempNode(KVPathUtil.getHaResponsePath(dh.getHostName()), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                        ClusterPathUtil.SUCCESS.getBytes(StandardCharsets.UTF_8));
                //change stage into waiting others
                HaConfigManager.getInstance().haWaitingOthers(id);
                //use zk to waiting other dble to response
                String errorMessage = isZKfinished(zkConn, KVPathUtil.getHaResponsePath(dh.getHostName()));

                //change stage into finished
                HaConfigManager.getInstance().haFinish(id, errorMessage, result);
                if (errorMessage != null && !"".equals(errorMessage)) {
                    mc.writeErrMessage(ErrorCode.ER_YES, errorMessage);
                    return false;
                }
            } finally {
                if (locked) {
                    distributeLock.release();
                }
                LOGGER.info("reload config: release distributeLock " + KVPathUtil.getConfChangeLockPath() + " from zk");
            }
        } catch (Exception e) {
            LOGGER.info("reload config using ZK failure", e);
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
            return false;
        }
        return true;
    }


    public static boolean disableWithCluster(int id, PhysicalDataHost dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getHaLockPath(dh.getHostName()),
                new HaInfo(dh.getHostName(),
                        ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                        HaInfo.HaType.DATAHOST_DISABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dataHost, please try again later.");
                HaConfigManager.getInstance().haFinish(id, "Other instance is changing the dataHost, please try again later.", null);
                return false;
            }
            //local set disable
            final String result = dh.disableHosts(subHostName, false);

            //update total dataSources status
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getHostName()), dh.getClusterHaJson());
            // update the notify value let other dble to notify
            ClusterHelper.setKV(ClusterPathUtil.getHaLockPath(dh.getHostName()),
                    new HaInfo(dh.getHostName(),
                            ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                            HaInfo.HaType.DATAHOST_DISABLE,
                            HaInfo.HaStatus.SUCCESS
                    ).toString());
            //write out self success message
            ClusterHelper.setKV(ClusterPathUtil.getSelfResponsePath(ClusterPathUtil.getHaLockPath(dh.getHostName())), ClusterPathUtil.SUCCESS);
            //change log stage into wait others
            HaConfigManager.getInstance().haWaitingOthers(id);
            //waiting for other dble to response
            String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getHaLockPath(dh.getHostName()) + SEPARATOR);
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
            ClusterHelper.cleanPath(ClusterPathUtil.getHaLockPath(dh.getHostName()) + SEPARATOR);
            distributeLock.release();
        }
        return true;
    }


    public static void setStatusToZK(String nodePath, CuratorFramework curator, String value) throws Exception {
        Stat stat = curator.checkExists().forPath(nodePath);
        if (null == stat) {
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), nodePath);
        }
        LOGGER.debug("ZkMultiLoader write file :" + nodePath + ", value :" + value);
        curator.setData().inBackground().forPath(nodePath, value.getBytes());
    }


    public static String isZKfinished(CuratorFramework zkConn, String preparePath) {
        try {
            List<String> preparedList = zkConn.getChildren().forPath(preparePath);
            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());

            StringBuilder errorMsg = new StringBuilder();
            while (preparedList.size() < onlineList.size()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
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
