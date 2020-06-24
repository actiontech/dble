/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
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
        LOGGER.info("resume start from command");
        if (ClusterConfig.getInstance().isClusterEnable() && !ClusterConfig.getInstance().useZkMode()) {
            try {
                KvBean value = ClusterHelper.getKV(ClusterPathUtil.getPauseShardingNodePath());
                if (value.getValue() == null || "".equals(value.getValue())) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No shardingNode paused");
                    return;
                }

                PauseInfo pauseInfo = new PauseInfo(value.getValue());
                if (!pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "This node is not the node which start pause");
                    return;
                }

                if (!PauseShardingNodeManager.getInstance().tryResume()) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No shardingNode paused");
                    return;
                }

                PauseShardingNodeManager.getInstance().resumeCluster();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage());
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
                return;
            }
        } else {
            if (!PauseShardingNodeManager.getInstance().tryResume()) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No shardingNode paused");
                return;
            }
        }

        OK.write(c);
    }
}
