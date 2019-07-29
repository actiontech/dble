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
        while (true) {
            while ((handler = frontHandlerQueue.poll()) != null) {

                //threadUsageStat start
                boolean useThreadUsageStat = false;
                String threadName = null;
                ThreadWorkUsage workUsage = null;
                long workStart = 0;
                if (DbleServer.getInstance().getConfig().getSystem().getUseThreadUsageStat() == 1) {
                    useThreadUsageStat = true;
                    threadName = Thread.currentThread().getName();
                    workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);

                    if (workUsage == null) {
                        workUsage = new ThreadWorkUsage();
                        DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                    }
                    workStart = System.nanoTime();
                }
                //handler data
                handler.handle();

                //threadUsageStat end
                if (useThreadUsageStat) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            }
        }
    }
}
