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
    private final List<Runnable> callbacks = new ArrayList<>();


    public ConnectionSerializableLock(long frontId) {
        this.frontId = frontId;

    }

    public synchronized boolean tryLock() {

        if (!working) {
            working = true;
            LOGGER.debug("locked id " + frontId + " success");
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

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(" unlock id " + frontId);

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

    private String printTrace() {
        StackTraceElement[] st = Thread.currentThread().getStackTrace();
        if (st == null) {
            return "";
        }
        StringBuilder sbf = new StringBuilder();
        for (StackTraceElement e : st) {
            if (sbf.length() > 0) {
                sbf.append(" <- ");
                sbf.append(System.getProperty("line.separator"));
            }
            sbf.append(java.text.MessageFormat.format("{0}.{1}() {2}", e.getClassName(), e.getMethodName(), e.getLineNumber()));
        }
        sbf.append("==============================================");
        return sbf.toString();
    }
}
