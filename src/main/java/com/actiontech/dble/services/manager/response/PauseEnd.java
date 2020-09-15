/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import com.actiontech.dble.util.StringUtil;
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

    public static void execute(final ManagerService c) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                resume(c);
            }
        });

    }


    public static void resume(ManagerService service) {
        LOGGER.info("resume start from command");
        if (ClusterConfig.getInstance().isClusterEnable()) {
            try {
                String value = ClusterHelper.getPathValue(ClusterPathUtil.getPauseResultNodePath());
                if (StringUtil.isEmpty(value)) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No shardingNode paused");
                    return;
                }

                if (!PauseShardingNodeManager.getInstance().tryResume()) {
                    OK.setMessage(("No shardingNode paused, But still notify cluster").getBytes());
                    return;
                }

                if (!PauseShardingNodeManager.getInstance().getDistributeLock()) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "other instance is in operation");
                    return;
                }

                try {
                    PauseShardingNodeManager.getInstance().resumeCluster();
                } finally {
                    PauseShardingNodeManager.getInstance().releaseDistributeLock();
                }

            } catch (Exception e) {
                LOGGER.warn("resume failed", e);
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage() == null ? e.toString() : e.getMessage());
                return;
            }
        } else {
            if (!PauseShardingNodeManager.getInstance().tryResume()) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No shardingNode paused");
                return;
            }
        }

        OK.write(service.getConnection());
    }
}
