/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * guarantee all packet in one connection are consumed one by one with order.
 *
 * @author dcy
 */
public final class ConnectionSerializableLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionSerializableLock.class);
    private boolean working = false;
    private final long frontId;
    private long index = 0;
    private final List<Runnable> callbacks = new ArrayList<>();


    public ConnectionSerializableLock(long frontId) {
        this.frontId = frontId;

    }

    public synchronized boolean tryLock() {

        if (!working) {
            working = true;
            LOGGER.debug("locked success. connection id : {} , index : {}", frontId, ++index);
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

    public synchronized void unLock() {
        if (working) {
            LOGGER.debug(" unlock success. connection id : {} , index : {}", frontId, index);
        }
        if (!working) {
            LOGGER.warn("find useless unlock.");
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


    }


}
