/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.PauseInfo;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.singleton.PauseShardingNodeManager;
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
        c.getClusterDelayService().markDoingOrDelay(true);
        OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                resume(c);
            }
        });

    }

    public static void resume(ManagerService service) {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            try {
                ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.PAUSE_RESUME);
                PauseInfo pauseInfo = clusterHelper.getPathValue(ClusterMetaUtil.getPauseResultNodePath()).map(ClusterValue::getData).orElse(null);
                if (pauseInfo == null) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "No shardingNode paused");
                    return;
                }
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("{}", pauseInfo);

                if (!PauseShardingNodeManager.getInstance().getDistributeLock()) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "other instance is in operation");
                    return;
                }

                try {
                    if (!PauseShardingNodeManager.getInstance().tryResume()) {
                        OK.setMessage(("No shardingNode paused, But still notify cluster").getBytes());
                        return;
                    }
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
