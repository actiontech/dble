/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.response.ShowBinlogStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/31.
 */
public class BinlogPauseStatusResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusResponse.class);

    private static final String CONFIG_PATH = ClusterPathUtil.getBinlogPauseStatus();


    public BinlogPauseStatusResponse(ClusterClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }


    @Override
    public void notifyProcess(KvBean configValue) throws Exception {

        //step 1 check if the block is from the server itself
        BinlogPause pauseInfo = new BinlogPause(configValue.getValue());
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        if (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceId())) {
            LOGGER.info("Self Notice,Do nothing return");
            return;
        }

        //step 2 if the flag is on than try to lock all the commit
        if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON && !KvBean.DELETE.equals(configValue.getChangeType())) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(false, true);
            LOGGER.info("start pause for binlog status");
            boolean isPaused = ShowBinlogStatus.waitAllSession();
            if (!isPaused) {
                ClusterHelper.cleanBackupLocked();
                ClusterHelper.setKV(ClusterPathUtil.getBinlogPauseStatusSelf(), "Error can't wait all session finished ");
                return;
            }
            try {
                ClusterHelper.setKV(ClusterPathUtil.getBinlogPauseStatusSelf(), ClusterPathUtil.SUCCESS);
            } catch (Exception e) {
                ClusterHelper.cleanBackupLocked();
                LOGGER.warn("create binlogPause instance failed", e);
            }
        } else if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.OFF) {
            LOGGER.info("clean resource for binlog status finish");
            //step 3 if the flag is off than try to unlock the commit
            ClusterHelper.cleanBackupLocked();
        }
    }


    @Override
    public void notifyCluster() throws Exception {
        return;
    }
}
