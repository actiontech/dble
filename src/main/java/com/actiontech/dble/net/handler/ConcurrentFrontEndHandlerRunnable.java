/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;

import java.util.Queue;

public class ConcurrentFrontEndHandlerRunnable implements Runnable {
    private final Queue<FrontendCommandHandler> frontHandlerQueue;

    public ConcurrentFrontEndHandlerRunnable(Queue<FrontendCommandHandler> frontHandlerQueue) {
        this.frontHandlerQueue = frontHandlerQueue;

    }

    @Override
    public void run() {
        FrontendCommandHandler handler;
        ThreadWorkUsage workUsage = null;
        if (DbleServer.getInstance().getConfig().getSystem().getUseThreadUsageStat() == 1) {
            String threadName = Thread.currentThread().getName();
            workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
        }
        while (true) {
            while ((handler = frontHandlerQueue.poll()) != null) {

                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }

                //handler data
                handler.handle();

                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            }
        }
    }
}
