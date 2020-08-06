package com.actiontech.dble.net.executor;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by szf on 2020/6/18.
 */
public class FrontendBlockRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendBlockRunnable.class);
    private final BlockingQueue<ServiceTask> frontNormalTasks;
    private final Queue<ServiceTask> frontPriorityTasks;

    public FrontendBlockRunnable(Queue frontEndTasks, Queue<ServiceTask> frontPriorityTasks) {
        this.frontNormalTasks = (BlockingQueue) frontEndTasks;
        this.frontPriorityTasks = frontPriorityTasks;
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
                task = frontPriorityTasks.poll();
                if (task == null) {
                    task = frontNormalTasks.take();
                }
                if (task.getService() == null) {
                    continue;
                }

                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }
                //handler data
                task.getService().execute(task);

                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("FrontendCommandHandler error.", e);
            }
        }
    }


}
