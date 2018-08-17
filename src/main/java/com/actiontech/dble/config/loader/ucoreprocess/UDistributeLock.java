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
    private static final int UCORE_ERROR_RETURN_COUNT = 3;
    private int errorCount = 0;
    private String path;
    private String value;
    private String session;

    private Thread renewThread;

    public UDistributeLock(String path, String value) {
        this.path = path;
        this.value = value;
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
            int time = 0;
            while ("".equals(sessionId)) {
                LOGGER.info(" lockKey's sessionId is empty, server will retry for 10 seconds later ");
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
                sessionId = ClusterUcoreSender.lockKey(this.path, value);
                if (time++ == 5) {
                    LOGGER.warn(" lockKey's sessionId is empty and have tried for 5 times, return false ");
                    return false;
                }
            }
            session = sessionId;
            errorCount = 0;
            if ("".equals(sessionId)) {
                return false;
            }
            renewThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    String sessionId = session;
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            LOGGER.info("renew lock of session  start:" + sessionId + " " + path);
                            if ("".equals(ClusterUcoreSender.getKey(path).getValue())) {
                                LOGGER.warn("renew lock of session  failure:" + sessionId + " " + path + ", the key is missing ");
                                // alert
                                renewThread.interrupt();
                            } else if (!ClusterUcoreSender.renewLock(sessionId)) {
                                LOGGER.warn("renew lock of session  failure:" + sessionId + " " + path);
                                // alert
                            } else {
                                LOGGER.info("renew lock of session  success:" + sessionId + " " + path);
                            }
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10000));
                        } catch (Exception e) {
                            LOGGER.info("renew lock of session  failure:" + sessionId + " " + path, e);
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
            if (errorCount == UCORE_ERROR_RETURN_COUNT) {
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
