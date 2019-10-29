/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class SerializableLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(SerializableLock.class);
    private AtomicBoolean working = new AtomicBoolean(false);

    private static final SerializableLock INSTANCE = new SerializableLock();

    public static SerializableLock getInstance() {
        return INSTANCE;
    }

    private SerializableLock() {
    }

    public void lock() {
        if (DbleServer.getInstance().getConfig().getSystem().getUseSerializableMode() == 1) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to get lock " + printTrack());
                LOGGER.debug("==============================================");
            }
            while (!working.compareAndSet(false, true)) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("locked " + printTrack());
                LOGGER.debug("==============================================");
            }
        }
    }

    public void unLock() {
        if (DbleServer.getInstance().getConfig().getSystem().getUseSerializableMode() == 1) {
            working.set(false);
        }
    }

    private String printTrack() {
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
        return sbf.toString();
    }
}
