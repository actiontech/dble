/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.DistributeLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by szf on 2018/1/31.
 */
public class ClusterGeneralDistributeLock extends DistributeLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterGeneralDistributeLock.class);
    private final int maxErrorCnt;
    private int errorCount = 0;
    private String session;

    //private Thread renewThread;

    public ClusterGeneralDistributeLock(String path, String value) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = 3;
    }

    public ClusterGeneralDistributeLock(String path, String value, int maxErrorCnt) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = maxErrorCnt;
    }

    @Override
    public void release() {
        if (session != null) {
            ClusterGeneralConfig.getInstance().getClusterSender().unlockKey(path, session);
        }
    }

    @Override
    public boolean acquire() {
        try {
            String sessionId = ClusterGeneralConfig.getInstance().getClusterSender().lock(path, value);
            if ("".equals(sessionId)) {
                errorCount++;
                if (errorCount == maxErrorCnt) {
                    throw new RuntimeException(" get lock from ucore error,ucore maybe offline ");
                }
                return false;
            }
            session = sessionId;
            errorCount = 0;
        } catch (Exception e) {
            LOGGER.warn(" get lock from ucore error", e);
            errorCount++;
            if (errorCount == maxErrorCnt) {
                throw new RuntimeException(" get lock from ucore error,ucore maybe offline ");
            }
            return false;
        }
        return true;
    }

}
