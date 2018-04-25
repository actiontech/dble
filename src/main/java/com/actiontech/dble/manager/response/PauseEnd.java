package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class PauseEnd {
    private static final OkPacket OK = new OkPacket();
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadMetaData.class);

    private PauseEnd() {
    }

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    public static void execute(ManagerConnection c) {

        if (DbleServer.getInstance().isUseUcore()) {
            try {
                UKvBean value = ClusterUcoreSender.getKey(UcorePathUtil.getPauseDataNodePath());
                PauseInfo pauseInfo = new PauseInfo(value.getValue());
                if (!pauseInfo.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                    c.writeErrMessage(1003, "This node is node the node which start pause");
                    return;
                }

                DbleServer.getInstance().getMiManager().resume();

                DbleServer.getInstance().getMiManager().getuDistributeLock().release();
                ClusterUcoreSender.deleteKVTree(UcorePathUtil.getPauseDataNodePath());
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getPauseDataNodePath(),
                        new PauseInfo(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), " ", PauseInfo.RESUME).toString());

                while (true) {
                    List<UKvBean> reponseList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getPauseResumePath());
                    List<UKvBean> onlineList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getOnlinePath());
                    if (reponseList.size() >= onlineList.size() - 1) {
                        ClusterUcoreSender.deleteKVTree(UcorePathUtil.getPauseDataNodePath());
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.warn(AlarmCode.CORE_CLUSTER_WARN + " ");
            }
        } else {
            DbleServer.getInstance().getMiManager().resume();
        }

        OK.write(c);
    }
}
