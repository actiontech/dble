/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NameableThreadFactory implements ThreadFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(NameableThreadFactory.class);
    private final ThreadGroup group;
    private final String namePrefix;
    private final AtomicInteger threadId;
    private final boolean isDaemon;

    public NameableThreadFactory(String name, boolean isDaemon) {
        SecurityManager s = System.getSecurityManager();
        this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = name;
        this.threadId = new AtomicInteger(0);
        this.isDaemon = isDaemon;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadId.getAndIncrement());
        t.setDaemon(isDaemon);
        //If more processing needs to be overridden class processing
        t.setUncaughtExceptionHandler((Thread threads, Throwable e) -> LOGGER.warn("unknown exception ", e));
        return t;
    }
}
