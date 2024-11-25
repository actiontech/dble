/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NameableThreadFactory implements ThreadFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(NameableThreadFactory.class);
    private final ThreadGroup group;
    private final String namePrefix;
    private String nameSuffix;
    private final AtomicInteger threadId;
    private final boolean isDaemon;

    public NameableThreadFactory(String namePrefix, boolean isDaemon) {
        SecurityManager s = System.getSecurityManager();
        this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = namePrefix;
        this.threadId = new AtomicInteger(0);
        this.isDaemon = isDaemon;
    }

    public NameableThreadFactory(String namePrefix, String nameSuffix, boolean isDaemon) {
        SecurityManager s = System.getSecurityManager();
        this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = namePrefix;
        this.nameSuffix = nameSuffix;
        this.threadId = new AtomicInteger(0);
        this.isDaemon = isDaemon;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, threadId.getAndIncrement() + "-" + namePrefix + (StringUtil.isBlank(nameSuffix) ? "" : nameSuffix));
        t.setDaemon(isDaemon);
        //If more processing needs to be overridden class processing
        t.setUncaughtExceptionHandler((Thread threads, Throwable e) -> LOGGER.warn("unknown exception ", e));
        return t;
    }
}
