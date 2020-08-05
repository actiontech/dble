/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general;

import com.actiontech.dble.cluster.DistributeLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by szf on 2018/1/31.
 */
public class ConsulDistributeLock extends DistributeLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulDistributeLock.class);
    private final int maxErrorCnt;
    private int errorCount = 0;
    private String session;
    private AbstractConsulSender sender;

    //private Thread renewThread;

    public ConsulDistributeLock(String path, String value, AbstractConsulSender sender) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = 3;
        this.sender = sender;
    }

    public ConsulDistributeLock(String path, String value, int maxErrorCnt, AbstractConsulSender sender) {
        this.path = path;
        this.value = value;
        this.maxErrorCnt = maxErrorCnt;
        this.sender = sender;
    }

    @Override
    public void release() {
        if (session != null) {
            sender.unlockKey(path, session);
        }
    }

    @Override
    public boolean acquire() {
        try {
            String sessionId = sender.lock(path, value);
            if ("".equals(sessionId)) {
                errorCount++;
                if (errorCount == maxErrorCnt) {
                    throw new RuntimeException(" get DistributeLock from ucore error,ucore maybe offline ");
                }
                LOGGER.warn(" get DistributeLock from ucore failed");
                return false;
            }
            session = sessionId;
            errorCount = 0;
        } catch (Exception e) {
            LOGGER.warn(" get DistributeLock from ucore error", e);
            errorCount++;
            if (errorCount >= maxErrorCnt) {
                throw new RuntimeException(" get DistributeLock from ucore error, ucore maybe offline ");
            }
            return false;
        }
        return true;
    }

}
