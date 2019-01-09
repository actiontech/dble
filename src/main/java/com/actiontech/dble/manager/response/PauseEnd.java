/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.ErrorCode;
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

    public static void execute(final ManagerConnection c) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                resume(c);
            }
        });

    }


    public static void resume(ManagerConnection c) {

        if (DbleServer.getInstance().isUseUcore()) {
            try {
                UKvBean value = ClusterUcoreSender.getKey(UcorePathUtil.getPauseDataNodePath());
                PauseInfo pauseInfo = new PauseInfo(value.getValue());
                if (!pauseInfo.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "This node is not the node which start pause");
                    return;
                }

                if (!DbleServer.getInstance().getMiManager().tryResume()) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No dataNode paused");
                    return;
                }

                DbleServer.getInstance().getMiManager().resumeCluster();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage());
            }
        } else {
            if (!DbleServer.getInstance().getMiManager().tryResume()) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No dataNode paused");
                return;
            }
        }

        OK.write(c);
    }
}
