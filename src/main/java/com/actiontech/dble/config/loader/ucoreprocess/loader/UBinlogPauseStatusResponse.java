/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.manager.response.ShowBinlogStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/31.
 */
public class UBinlogPauseStatusResponse implements UcoreXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(UBinlogPauseStatusResponse.class);

    private static final String CONFIG_PATH = UcorePathUtil.getBinlogPauseStatus();


    public UBinlogPauseStatusResponse(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }


    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {

        //step 1 check if the block is from the server itself
        BinlogPause pauseInfo = new BinlogPause(configValue.getValue());
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        if (pauseInfo.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
            LOGGER.info("Self Notice,Do nothing return");
            return;
        }

        //step 2 if the flag is on than try to lock all the commit
        if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON && !UKvBean.DELETE.equals(configValue.getChangeType())) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(false, true);
            LOGGER.info("start pause for binlog status");
            boolean isPaused = ShowBinlogStatus.waitAllSession();
            if (!isPaused) {
                cleanResource();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getBinlogPauseStatusSelf(), "Error can't wait all session finished ");
                return;
            }
            try {
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getBinlogPauseStatusSelf(), UcorePathUtil.SUCCESS);
            } catch (Exception e) {
                cleanResource();
                LOGGER.warn("create binlogPause instance failed", e);
            }
        } else if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.OFF) {
            LOGGER.info("clean resource for binlog status finish");
            //step 3 if the flag is off than try to unlock the commit
            cleanResource();
        }
    }


    private synchronized void cleanResource() {
        if (DbleServer.getInstance().getBackupLocked() != null) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        return;
    }
}
