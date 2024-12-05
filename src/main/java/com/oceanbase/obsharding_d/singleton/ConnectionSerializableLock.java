/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.services.FrontendService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * guarantee all packet in one connection are consumed one by one with order.
 *
 * @author dcy
 */
public final class ConnectionSerializableLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionSerializableLock.class);
    private boolean working = false;
    private final long frontId;
    private volatile long index = -1;
    private final Set<Runnable> callbacks = new LinkedHashSet<>();
    private final FrontendService frontendService;


    public ConnectionSerializableLock(long frontId, FrontendService frontendService) {
        this.frontId = frontId;
        this.frontendService = frontendService;
    }

    public synchronized boolean tryLock() {

        if (!working) {
            working = true;
            index = frontendService.getCurrentTaskIndex();
            LOGGER.debug("locked success. connection id : {} , index : {}", frontId, index);
            return true;
        }
        return false;
    }

    public synchronized void addListener(Runnable callback) throws RuntimeException {
        if (working) {
            this.callbacks.add(callback);
        } else {
            throw new IllegalStateException("can't register listener while isn't locking");
        }

    }


    public synchronized boolean isLocking() {
        return working;
    }

    /**
     * should be unlocked if is locking.Then the next packet can begin to processing.
     * notice: lock once and unlock twice is a bad idea
     */
    public void unLock() {
        if (index == -1) {
            return;
        }
        synchronized (this) {
            if (working) {
                LOGGER.debug(" unlock success. connection id : {} , index : {}", frontId, index);
            }
            if (!working) {
                //locked before
                if (frontendService.getCurrentTaskIndex() == index) {
                    LOGGER.warn("find useless unlock. connection id : {} , index : {}", frontId, index); // maybe something wrong
                }
                if (!frontendService.isDoingTask() && frontendService.getRecvTaskQueueSize() > 0) {
                    LOGGER.warn("notify frontend work.service:{}", frontendService); // maybe something wrong
                    frontendService.notifyTaskThread();
                }
                return;
            }
            working = false;


            for (Runnable callback : this.callbacks) {
                try {
                    callback.run();
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }

            this.callbacks.clear();

            if (!frontendService.isDoingTask() && frontendService.getRecvTaskQueueSize() > 0) {
                frontendService.notifyTaskThread();
            }

        }

    }


}
