package com.actiontech.dble.util;

import com.actiontech.dble.singleton.ThreadChecker;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

public class NameableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    protected String name;
    private ThreadChecker threadChecker = null;

    public NameableScheduledThreadPoolExecutor(String name, int corePoolSize, ThreadFactory threadFactory, ThreadChecker threadChecker) {
        super(corePoolSize, threadFactory);
        this.name = name;
        this.threadChecker = threadChecker;
    }

    public String getName() {
        return name;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        if (threadChecker != null) {
            threadChecker.startExec(t);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (threadChecker != null) {
            threadChecker.endExec();
        }
    }

    @Override
    protected void terminated() {
        super.terminated();
        if (threadChecker != null) {
            threadChecker.terminated();
        }
    }
}
