/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DirectoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.response.ShowBinlogStatus;
import com.actiontech.dble.util.KVPathUtil;
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
        currZkPath = KVPathUtil.getBinlogPauseStatus();
        zookeeperListen.addWatch(currZkPath, this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        DirectoryInf statusDirectory = new ZkDirectoryImpl(currZkPath, null);
        this.getTreeDirectory(currZkPath, KVPathUtil.BINLOG_PAUSE_STATUS, statusDirectory);
        ZkDataImpl zkDdata = (ZkDataImpl) statusDirectory.getSubordinateInfo().get(0);
        String strPauseInfo = zkDdata.getDataValue();
        LOGGER.info("BinlogPauseStatusListener notifyProcess zk to object  :" + strPauseInfo);

        BinlogPause pauseInfo = new BinlogPause(strPauseInfo);
        String myID = SystemConfig.getInstance().getInstanceId();
        if (pauseInfo.getFrom().equals(myID)) {
            return true; //self node
        }
        String instancePath = ZKPaths.makePath(KVPathUtil.getBinlogPauseInstance(), myID);
        if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(false, true);
            boolean isPaused = ShowBinlogStatus.waitAllSession();
            if (!isPaused) {
                cleanResource(instancePath);
            }
            try {
                ZKUtils.createTempNode(KVPathUtil.getBinlogPauseInstance(), myID, String.valueOf(isPaused).getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                cleanResource(instancePath);
                LOGGER.warn("create binlogPause instance failed", e);
            }
        } else if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.OFF) {
            cleanResource(instancePath);
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
