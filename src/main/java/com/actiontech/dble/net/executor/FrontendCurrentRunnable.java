/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.executor;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.Service;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.singleton.ConnectionAssociateThreadManager;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;

/**
 * Created by szf on 2020/7/9.
 */
public class FrontendCurrentRunnable implements FrontendRunnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendCurrentRunnable.class);

    private final Deque<ServiceTask> frontNormalTasks;
    private final ThreadContext threadContext = new ThreadContext();

    public FrontendCurrentRunnable(Deque<ServiceTask> frontEndTasks) {
        this.frontNormalTasks = frontEndTasks;
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
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("interrupt thread:{},frontNormalTasks:{}", Thread.currentThread().toString(), frontNormalTasks);
                    break;
                }
                task = frontNormalTasks.poll();

                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }
                if (task != null) {
                    //handler data
                    if (task.getService() == null) {
                        continue;
                    }
                    final Service service = task.getService();
                    try {
                        ConnectionAssociateThreadManager.getInstance().put(service);
                        task.getService().execute(task, threadContext);
                    } finally {
                        ConnectionAssociateThreadManager.getInstance().remove(service);
                    }
                }

                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (Throwable t) {
                LOGGER.warn("Unknown error:", t);
            }
        }
    }
}
