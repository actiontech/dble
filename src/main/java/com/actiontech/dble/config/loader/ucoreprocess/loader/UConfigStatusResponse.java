package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterParamCfg;
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

    private static final String CONFIG_PATH = UcorePathUtil.getConfStatusPath();

    public UConfigStatusResponse(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean pathValue) throws Exception {

        ClusterDelayProvider.delayAfterGetNotice();
        if (DbleServer.getInstance().getFrontProcessors() != null) {
            //step 1 check if the change is from itself
            LOGGER.info("notify " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
            ConfStatus status = new ConfStatus(pathValue.getValue());
            if (status.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                //self node
                return;
            }

            //check if the reload is already be done by this node
            if (!"".equals(ClusterUcoreSender.getKey(UcorePathUtil.getSelfConfStatusPath()).getValue()) ||
                    "".equals(ClusterUcoreSender.getKey(UcorePathUtil.getConfStatusPath()).getValue())) {
                return;
            }

            //step 2 check the change type /rollback /reload
            if (status.getStatus() == ConfStatus.Status.ROLLBACK) {
                LOGGER.info("rollback " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                try {
                    ClusterDelayProvider.delayBeforeSlaveRollback();
                    RollbackConfig.rollback();
                    ClusterDelayProvider.delayAfterSlaveRollback();
                    ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), UcorePathUtil.SUCCESS);
                } catch (Exception e) {
                    String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                    ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), errorinfo);
                }
                return;
            }

            //step 3 reload the config and set the self config status
            try {
                ClusterDelayProvider.delayBeforeSlaveReload();
                if (status.getStatus() == ConfStatus.Status.RELOAD_ALL) {
                    LOGGER.info("reload_all " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                    ReloadConfig.reloadAll(Integer.parseInt(status.getParams()));
                } else {
                    LOGGER.info("reload " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                    ReloadConfig.reload();
                }
                ClusterDelayProvider.delayAfterSlaveReload();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), UcorePathUtil.SUCCESS);
            } catch (Exception e) {
                String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getSelfConfStatusPath(), errorinfo);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {

    }
}
