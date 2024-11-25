/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.executor;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.btrace.provider.OBsharding_DThreadPoolProvider;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.mysql.WriteToBackendTask;
import com.oceanbase.obsharding_d.net.service.Service;
import com.oceanbase.obsharding_d.singleton.ConnectionAssociateThreadManager;
import com.oceanbase.obsharding_d.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class WriteToBackendRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteToBackendRunnable.class);
    private final BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;
    private final ThreadContext threadContext = new ThreadContext();

    public WriteToBackendRunnable(BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue) {
        this.writeToBackendQueue = writeToBackendQueue;
    }

    public ThreadContextView getThreadContext() {
        return threadContext;
    }

    @Override
    public void run() {
        ThreadWorkUsage workUsage = null;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            String threadName = Thread.currentThread().getName();
            workUsage = new ThreadWorkUsage();
            OBsharding_DServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
        }
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                OBsharding_DServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("interrupt thread:{},writeToBackendQueue:{}", Thread.currentThread().toString(), writeToBackendQueue);
                break;
            }
            try {
                List<WriteToBackendTask> tasks = writeToBackendQueue.take();
                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }


                threadContext.setDoingTask(true);
                try {

                    //execute the tasks
                    for (WriteToBackendTask task : tasks) {
                        final Service service = task.getService();
                        try {
                            ConnectionAssociateThreadManager.getInstance().put(service);
                            OBsharding_DThreadPoolProvider.beginProcessWriteToBackendTask();
                            task.execute();
                            ThreadPoolStatistic.getWriteToBackend().getCompletedTaskCount().increment();
                        } finally {
                            ConnectionAssociateThreadManager.getInstance().remove(service);
                        }
                    }
                } finally {
                    threadContext.setDoingTask(false);
                }
                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (InterruptedException e) {
                OBsharding_DServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("interrupt thread:{},concurrentBackQueue:{}", Thread.currentThread().toString(), writeToBackendQueue, e);
                break;
            } catch (Throwable t) {
                LOGGER.warn("Unknown error:", t);
            }
        }

    }
}
