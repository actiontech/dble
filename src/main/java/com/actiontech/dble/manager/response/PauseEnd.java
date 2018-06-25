/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
                    c.writeErrMessage(1003, "This node is not the node which start pause");
                    return;
                }

                if (!DbleServer.getInstance().getMiManager().tryResume()) {
                    c.writeErrMessage(1003, "There is other connection is resume the dble");
                    return;
                }

                DbleServer.getInstance().getMiManager().resumeCluster();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage());
            }
        } else {
            if (!DbleServer.getInstance().getMiManager().tryResume()) {
                c.writeErrMessage(1003, "There is other connection is resume the dble");
                return;
            }
        }

        OK.write(c);
    }
}
