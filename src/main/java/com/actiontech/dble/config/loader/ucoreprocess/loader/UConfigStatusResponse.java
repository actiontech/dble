package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.manager.response.ReloadConfig;
import com.actiontech.dble.manager.response.RollbackConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/31.
 */
public class UConfigStatusResponse implements UcoreXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(UBinlogPauseStatusResponse.class);

    public static final String SUCCESS = "SUCCESS";

    private static final String CONFIG_PATH = UcorePathUtil.getConfStatusPath();

    public UConfigStatusResponse(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean pathValue) throws Exception {
        LOGGER.debug("get into UConfigStatusResponse the value is " + pathValue);
        if (DbleServer.getInstance().getProcessors() != null) {
            //step 1 check if the change is from itself
            ConfStatus status = new ConfStatus(pathValue.getValue());
            if (status.getFrom().equals(UcoreConfig.getInstance().getValue(UcoreParamCfg.UCORE_CFG_MYID))) {
                return; //self node
            }
            //check if the reload is already be done by this node
            if (!"".equals(ClusterUcoreSender.getKey(UcorePathUtil.getSelfConfStatusPath()).getValue()) ||
                    "".equals(ClusterUcoreSender.getKey(UcorePathUtil.getConfStatusPath()).getValue())) {
                return;
            }
            LOGGER.debug("reload check end " + pathValue);
            //step 2 check the change type /rollback /reload
            if (status.getStatus() == ConfStatus.Status.ROLLBACK) {
                try {
                    RollbackConfig.rollback();
                    ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), UConfigStatusResponse.SUCCESS);
                } catch (Exception e) {
                    String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                    ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), errorinfo);
                }
                return;
            }

            //step 3 reload the config and set the self config status
            try {
                if (status.getStatus() == ConfStatus.Status.RELOAD_ALL) {
                    ReloadConfig.reloadAll(Integer.parseInt(status.getParams()));
                } else {
                    ReloadConfig.reload();
                }
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), UConfigStatusResponse.SUCCESS);
            } catch (Exception e) {
                String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), errorinfo);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {

    }

    @Override
    public void notifyProcessWithKey(String key, String value) throws Exception {
        return;
    }
}
