/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.executor;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

/**
 * Created by szf on 2020/7/9.
 */
public class BackendCurrentRunnable implements BackendRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendCurrentRunnable.class);

    private final Queue<ServiceTask> concurrentBackQueue;
    private final ThreadContext threadContext = new ThreadContext();

    public BackendCurrentRunnable(Queue<ServiceTask> concurrentBackQueue) {
        this.concurrentBackQueue = concurrentBackQueue;
    }


    @Override
    public ThreadContextView getThreadContext() {
        return threadContext;
    }

    @Override
    public void run() {
        ServiceTask task;
        ThreadWorkUsage workUsage = null;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            String threadName = Thread.currentThread().getName();
            workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
        }
        while (true) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                    LOGGER.debug("interrupt thread:{},concurrentBackQueue:{}", Thread.currentThread().toString(), concurrentBackQueue);
                    break;
                }
                while ((task = concurrentBackQueue.poll()) != null) {
                    //threadUsageStat start
                    long workStart = 0;
                    if (workUsage != null) {
                        workStart = System.nanoTime();
                    }
                    task.getService().execute(task, threadContext);
                    //threadUsageStat end
                    if (workUsage != null) {
                        workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                    }
                }
            } catch (Throwable t) {
                LOGGER.warn("Unknown error:", t);
            }
        }
    }
}
