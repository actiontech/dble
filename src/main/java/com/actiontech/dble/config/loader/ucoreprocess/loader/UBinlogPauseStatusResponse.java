package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.log.alarm.AlarmCode;
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
        if (pauseInfo.getFrom().equals(UcoreConfig.getInstance().getValue(UcoreParamCfg.UCORE_CFG_MYID))) {
            return;
        }

        //step 2 if the flag is on than try to lock all the commit
        if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON && configValue.getChangeType() != UKvBean.DELETE) {
            DbleServer.getInstance().getBackupLocked().compareAndSet(false, true);
            boolean isPaused = ShowBinlogStatus.waitAllSession();
            if (!isPaused) {
                cleanResource();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getBinlogPauseStatusSelf(), "Error can't wait all session finished ");
                return;
            }
            try {
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getBinlogPauseStatusSelf(), "true");
            } catch (Exception e) {
                cleanResource();
                LOGGER.warn(AlarmCode.CORE_ZK_WARN + "create binlogPause instance failed", e);
            }
        } else if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.OFF) {
            //step 3 if the flag is off than try to unlock the commit
            cleanResource();
        }
    }


    private synchronized void cleanResource() {
        DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
    }

    @Override
    public void notifyCluster() throws Exception {
        return;
    }
}
