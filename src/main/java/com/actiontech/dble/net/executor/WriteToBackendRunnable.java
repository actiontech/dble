/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.executor;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.WriteToBackendTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class WriteToBackendRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteToBackendRunnable.class);
    private final BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;

    public WriteToBackendRunnable(BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue) {
        this.writeToBackendQueue = writeToBackendQueue;
    }

    @Override
    public void run() {
        ThreadWorkUsage workUsage = null;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            String threadName = Thread.currentThread().getName();
            workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
        }
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                LOGGER.debug("interrupt thread:{},concurrentBackQueue:{}", Thread.currentThread().toString(), writeToBackendQueue);
                break;
            }
            try {
                List<WriteToBackendTask> tasks = writeToBackendQueue.take();
                if (Thread.currentThread().isInterrupted()) {
                    writeToBackendQueue.offer(tasks);
                    DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                    LOGGER.debug("interrupt thread:{},concurrentBackQueue:{}", Thread.currentThread().toString(), writeToBackendQueue);
                    break;
                }
                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }

                //execute the tasks
                for (WriteToBackendTask task : tasks) {
                    task.execute();
                }

                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (InterruptedException e) {
                DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                LOGGER.debug("interrupt thread:{},concurrentBackQueue:{}", Thread.currentThread().toString(), writeToBackendQueue);
                LOGGER.warn("FrontendCommandHandler error.", e);
                break;
            }
        }

    }
}
