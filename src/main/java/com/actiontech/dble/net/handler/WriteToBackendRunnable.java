/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.mysql.WriteToBackendTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class WriteToBackendRunnable implements Runnable {
    private final BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;

    public WriteToBackendRunnable(BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue) {
        this.writeToBackendQueue = writeToBackendQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                List<WriteToBackendTask> tasks = writeToBackendQueue.take();
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
                for (WriteToBackendTask task : tasks) {
                    task.execute();
                }
                //threadUsageStat end
                if (useThreadUsageStat) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("FrontendCommandHandler error.", e);
            }
        }

    }
}
