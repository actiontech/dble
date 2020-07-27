/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class SerializableLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(SerializableLock.class);
    private AtomicBoolean working = new AtomicBoolean(false);
    private AtomicLong id = new AtomicLong(0);

    private static final SerializableLock INSTANCE = new SerializableLock();

    public static SerializableLock getInstance() {
        return INSTANCE;
    }

    private SerializableLock() {
    }

    public void lock(long frontId) {
        if (SystemConfig.getInstance().getUseSerializableMode() == 1) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to get lock id " + frontId + ", trace" + printTrace());
            }
            while (!working.compareAndSet(false, true)) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            this.id.set(frontId);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("locked id " + frontId + " success, trace " + printTrace());
            }
        }
    }

    public void unLock(long frontId) {
        if (SystemConfig.getInstance().getUseSerializableMode() == 1) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try unlock id " + frontId + ", trace" + printTrace());
            }
            if (this.id.get() == frontId) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("unlocked id " + frontId + " success, trace " + printTrace());
                }
                working.set(false);
            }
        }
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
