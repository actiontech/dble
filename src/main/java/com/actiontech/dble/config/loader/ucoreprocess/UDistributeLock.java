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
        ClusterUcoreSender.unlockKey(path, session);
    }

    public void deleteTree() {
        if (renewThread != null) {
            renewThread.interrupt();
        }
        ClusterUcoreSender.deleteKVTree(this.path + "/");
    }

    public boolean acquire() {
        try {

            final String sessionId = ClusterUcoreSender.lockKey(this.path, value);
            session = sessionId;
            if ("".equals(sessionId)) {
                return false;
            }
            renewThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            LOGGER.info("renew lock of session  start:" + sessionId + " " + path);
                            ClusterUcoreSender.renewLock(sessionId);
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10000));
                            LOGGER.info("renew lock of session  success:" + sessionId + " " + path);
                        } catch (Exception e) {
                            LOGGER.info("renew lock of session  failure:" + sessionId + " " + path);
                        }
                    }
                }
            });
            renewThread.start();
        } catch (Exception e) {
            LOGGER.info("get lock from ucore error", e);
            return false;
        }
        return true;
    }


}
