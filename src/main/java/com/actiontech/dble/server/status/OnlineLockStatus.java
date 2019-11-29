/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.status;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.view.CKVStoreRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.cluster.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class OnlineLockStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineLockStatus.class);
    private volatile DistributeLock onlineLock = null;
    private volatile boolean onlineInited = false;
    private volatile boolean mainThreadTryed = false;

    private OnlineLockStatus() {
    }

    private static final OnlineLockStatus INSTANCE = new OnlineLockStatus();

    public static OnlineLockStatus getInstance() {
        return INSTANCE;
    }

    /**
     * mainThread call this function to init the online status for the first
     *
     * @return
     * @throws IOException
     */
    public synchronized boolean mainThreadInitClusterOnline() throws IOException {
        mainThreadTryed = true;
        return clusterOnlinInit();
    }


    /**
     * when the dble start with a cluster online failure,try to init again in the node listener
     *
     * @throws IOException
     */
    public void nodeListenerInitClusterOnline() throws IOException {
        if (mainThreadTryed) {
            clusterOnlinInit();
        }
    }

    /**
     * only init in first try of cluster online init
     * when the init finished the rebuild is give to ClusterOffLineListener
     *
     * @return
     * @throws IOException
     */
    private synchronized boolean clusterOnlinInit() throws IOException {
        if (!onlineInited) {
            return false;
        }
        LOGGER.info("rebuild metaUcoreInit");
        //check if the online mark is on than delete the mark and renew it
        ClusterHelper.cleanKV(ClusterPathUtil.getOnlinePath(ClusterGeneralConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)));
        if (onlineLock != null) {
            onlineLock.release();
        }
        onlineLock = new DistributeLock(ClusterPathUtil.getOnlinePath(ClusterGeneralConfig.getInstance().
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

    /**
     * only be called when the ClusterOffLineListener find the self online status is missing
     *
     * @throws IOException
     */
    public synchronized boolean rebuildOnline() {
        if (onlineInited) {
            if (onlineLock != null) {
                onlineLock.release();
            }
            onlineLock = new DistributeLock(ClusterPathUtil.getOnlinePath(ClusterGeneralConfig.getInstance().
                    getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                    toString(), 6);
            int time = 0;
            while (!onlineLock.acquire()) {
                time++;
                if (time == 3) {
                    LOGGER.warn(" onlineLock failed and have tried for 3 times");
                    return false;
                }
                // rebuild is triggered by online missing ,no wait for too long
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
            Repository newViewRepository = new CKVStoreRepository();
            DbleServer.getInstance().getTmManager().setRepository(newViewRepository);
            Map<String, Map<String, String>> viewCreateSqlMap = newViewRepository.getViewCreateSqlMap();
            DbleServer.getInstance().getTmManager().reloadViewMeta(viewCreateSqlMap);
            return true;
        }
        return false;
    }
}
