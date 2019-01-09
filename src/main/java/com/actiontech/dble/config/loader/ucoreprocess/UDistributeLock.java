/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * Created by szf on 2018/1/31.
 */
public class UDistributeLock {

    protected static final Logger LOGGER = LoggerFactory.getLogger(UDistributeLock.class);
    private final int maxErrorCnt;
    private int errorCount = 0;
    private String path;
    private String value;
    private String session;

    private Thread renewThread;

    public UDistributeLock(String path, String value) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = 3;
    }

    public UDistributeLock(String path, String value, int maxErrorCnt) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = maxErrorCnt;
    }


    public void release() {
        if (renewThread != null) {
            renewThread.interrupt();
        }
        if (session != null) {
            ClusterUcoreSender.unlockKey(path, session);
        }
    }

    public boolean acquire() {
        try {
            String sessionId = ClusterUcoreSender.lockKey(this.path, value);
            if ("".equals(sessionId)) {
                errorCount++;
                if (errorCount == maxErrorCnt) {
                    throw new RuntimeException(" get lock from ucore error,ucore maybe offline ");
                }
                return false;
            }
            session = sessionId;
            errorCount = 0;
            renewThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    String sessionId = session;
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            LOGGER.debug("renew lock of session  start:" + sessionId + " " + path);
                            if ("".equals(ClusterUcoreSender.getKey(path).getValue())) {
                                LOGGER.warn("renew lock of session  failure:" + sessionId + " " + path + ", the key is missing ");
                                // alert
                                renewThread.interrupt();
                            } else if (!ClusterUcoreSender.renewLock(sessionId)) {
                                LOGGER.warn("renew lock of session  failure:" + sessionId + " " + path);
                                // alert
                            } else {
                                LOGGER.debug("renew lock of session  success:" + sessionId + " " + path);
                            }
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10000));
                        } catch (Exception e) {
                            LOGGER.warn("renew lock of session  failure:" + sessionId + " " + path, e);
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5000));
                        }
                    }
                }
            });
            renewThread.setName("UCORE_RENEW_" + path);
            renewThread.start();
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
