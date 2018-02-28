/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;

import java.util.concurrent.BlockingQueue;

public class FrondEndRunnable implements Runnable {
    private final BlockingQueue<FrontendCommandHandler> frontHandlerQueue;
    public FrondEndRunnable(BlockingQueue<FrontendCommandHandler> frontHandlerQueue) {
        this.frontHandlerQueue = frontHandlerQueue;

    }

    @Override
    public void run() {
        FrontendCommandHandler handler;
        while (true) {
            try {
                handler = frontHandlerQueue.take();
                String threadName = Thread.currentThread().getName();
                ThreadWorkUsage workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);
                if (workUsage == null) {
                    workUsage = new ThreadWorkUsage();
                    DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                }
                long workStart = System.nanoTime();
                handler.handle();
                workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
