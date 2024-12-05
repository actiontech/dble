/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.logic;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.path.PathMeta;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.Empty;
import com.oceanbase.obsharding_d.cluster.values.FeedBackType;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.services.manager.response.ShowBinlogStatus;
import com.oceanbase.obsharding_d.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class BinlogClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(BinlogClusterLogic.class);

    BinlogClusterLogic() {
        super(ClusterOperation.BINGLOG);
    }

    public void executeBinlogPauseDeleteEvent(ClusterEvent<Empty> event) {
        String instanceName = event.getValue().getInstanceName();
        if (StringUtils.isEmpty(instanceName)) {
            return;
        }
        if (instanceName.equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("Self Notice,Do nothing return");
            return;
        }
        // delete node
        OBsharding_DServer.getInstance().getBackupLocked().compareAndSet(true, false);
    }

    public void executeBinlogPauseEvent(ClusterEvent<Empty> event) throws Exception {
        String instanceName = event.getValue().getInstanceName();
        if (StringUtils.isEmpty(instanceName)) {
            return;
        }
        if (instanceName.equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("Self Notice,Do nothing return");
            return;
        }

        //step 2  try to lock all the commit
        OBsharding_DServer.getInstance().getBackupLocked().compareAndSet(false, true);
        LOGGER.info("start to pause for binlog status");
        boolean isPaused = ShowBinlogStatus.waitAllSession();
        if (!isPaused) {
            OBsharding_DServer.getInstance().getBackupLocked().compareAndSet(true, false);
            clusterHelper.createSelfTempNode(ClusterPathUtil.getBinlogPauseStatusPath(), FeedBackType.ofError("Error can't wait all session finished "));
            return;
        }
        try {
            clusterHelper.createSelfTempNode(ClusterPathUtil.getBinlogPauseStatusPath(), FeedBackType.SUCCESS);
        } catch (Exception e) {
            OBsharding_DServer.getInstance().getBackupLocked().compareAndSet(true, false);
            LOGGER.warn("fail to create binlogPause instance", e);
        }
    }

    public void checkBinlogStatusRelease(String crashNode) {
        try {

            //check the latest bing_log status
            Optional<String> fromNode = clusterHelper.getPathValue(ClusterMetaUtil.getBinlogPauseStatusPath()).map(ClusterValue::getInstanceName);
            if (!fromNode.isPresent()) {
                OBsharding_DServer.getInstance().getBackupLocked().compareAndSet(true, false);
            } else if (crashNode.equals(fromNode.get())) {
                OBsharding_DServer.getInstance().getBackupLocked().compareAndSet(true, false);
                PathMeta<FeedBackType> myselfPath = ClusterMetaUtil.getBinlogPauseStatusSelfPath();
                Boolean myself = ClusterHelper.isExist(myselfPath.getPath());
                boolean needDelete = true;
                long beginTime = TimeUtil.currentTimeMillis();
                long timeout = ClusterConfig.getInstance().getShowBinlogStatusTimeout();
                while (!Boolean.TRUE.equals(myself)) {
                    //wait 2* timeout to release itself
                    if (TimeUtil.currentTimeMillis() > beginTime + 2 * timeout) {
                        LOGGER.warn("checkExists of " + myselfPath + " time out");
                        needDelete = false;
                        break;
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    myself = ClusterHelper.isExist(myselfPath.getPath());
                }
                if (needDelete) {
                    ClusterHelper.cleanKV(myselfPath);
                }
                LOGGER.warn(" service instance[" + crashNode + "] has crashed. " +
                        "Please manually make sure node [" + ClusterMetaUtil.getBinlogPauseStatusPath() + "] status in cluster " +
                        "after every instance received this message");
                ClusterHelper.cleanPath(ClusterPathUtil.getBinlogPauseStatusPath());
                ClusterHelper.cleanKV(ClusterPathUtil.getBinlogPauseLockPath());
            }
        } catch (Exception e) {
            LOGGER.warn(" server offline binlog status check error: ", e);
        }
    }

}
