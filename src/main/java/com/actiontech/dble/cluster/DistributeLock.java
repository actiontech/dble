/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by szf on 2018/1/31.
 */
public class DistributeLock {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DistributeLock.class);
    private final int maxErrorCnt;
    private int errorCount = 0;
    private String path;
    private String value;
    private String session;

    //private Thread renewThread;

    public DistributeLock(String path, String value) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = 3;
    }

    public DistributeLock(String path, String value, int maxErrorCnt) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = maxErrorCnt;
    }


    public void release() {
        if (session != null) {
            ClusterHelper.unlockKey(path, session);
        }
    }

    public boolean acquire() {
        try {
            String sessionId = ClusterHelper.lock(this.path, value);
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

    public String getPath() {
        return path;
    }
}
