/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkData;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.response.ShowBinlogStatus;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Created by huqing.yan on 2017/5/25.
 */
public class BinlogPauseStatusListener extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusListener.class);
    private final String currZkPath;

    public BinlogPauseStatusListener(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        currZkPath = ClusterPathUtil.getBinlogPauseStatus();
        zookeeperListen.addWatch(currZkPath, this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        ZkData data = this.getTreeDirectory(currZkPath, ClusterPathUtil.BINLOG_PAUSE_STATUS, false);
        String strPauseInfo = data.getValue();
        LOGGER.info("BinlogPauseStatusListener notifyProcess zk to object  :" + strPauseInfo);

        BinlogPause pauseInfo = new BinlogPause(strPauseInfo);
        String instanceName = SystemConfig.getInstance().getInstanceName();
        if (pauseInfo.getFrom().equals(instanceName)) {
            return true; //self node
        }
        if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(false, true);
            boolean isPaused = ShowBinlogStatus.waitAllSession();
            if (!isPaused) {
                ClusterHelper.cleanBackupLocked();
                ZKUtils.createTempNode(currZkPath, instanceName, "Error can't wait all session finished".getBytes(StandardCharsets.UTF_8));
                return true;
            }
            try {
                ZKUtils.createTempNode(currZkPath, instanceName, ClusterPathUtil.SUCCESS.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                ClusterHelper.cleanBackupLocked();
                LOGGER.warn("create binlogPause instance failed", e);
            }
        } else if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.OFF) {
            LOGGER.info("clean resource for binlog status finish");
            //step 3 if the flag is off than try to unlock the commit
            ClusterHelper.cleanBackupLocked();
            cleanResource(ZKPaths.makePath(currZkPath, instanceName));
        }
        return true;
    }

    private synchronized void cleanResource(String instancePath) {
        LOGGER.info("BinlogPauseStatusListener cleanResource");
        try {
            if (this.getCurator().checkExists().forPath(instancePath) != null) {
                this.getCurator().delete().forPath(instancePath);
            }
        } catch (Exception e) {
            LOGGER.warn("delete binlogPause instance failed", e);
        } finally {
            DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
        }
    }
}
