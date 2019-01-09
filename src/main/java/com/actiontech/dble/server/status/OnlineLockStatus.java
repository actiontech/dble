/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.status;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UDistributeLock;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class OnlineLockStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineLockStatus.class);
    private volatile UDistributeLock onlineLock = null;
    private volatile boolean onlineInited = false;

    private OnlineLockStatus() {
    }

    private static final OnlineLockStatus INSTANCE = new OnlineLockStatus();

    public static OnlineLockStatus getInstance() {
        return INSTANCE;
    }

    public boolean metaUcoreInit(boolean init) throws IOException {
        if (!init && !onlineInited) {
            return false;
        }
        //check if the online mark is on than delete the mark and renew it
        ClusterUcoreSender.deleteKV(UcorePathUtil.getOnlinePath(UcoreConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)));
        if (onlineLock != null) {
            onlineLock.release();
        }
        onlineLock = new UDistributeLock(UcorePathUtil.getOnlinePath(UcoreConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                "" + System.currentTimeMillis(), 6);
        int time = 0;
        while (!onlineLock.acquire()) {
            time++;
            if (time == 5) {
                LOGGER.warn(" onlineLock failed and have tried for 5 times, return false ");
                throw new IOException("set online status failed");
            }
            LOGGER.info("onlineLock failed, server will retry for 10 seconds later ");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
        }
        onlineInited = true;
        return true;
    }

}
