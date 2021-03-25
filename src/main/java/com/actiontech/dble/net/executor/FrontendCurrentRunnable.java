package com.actiontech.dble.net.executor;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;

import java.util.Queue;

/**
 * Created by szf on 2020/7/9.
 */
public class FrontendCurrentRunnable implements Runnable {

    private final Queue<ServiceTask> frontNormalTasks;

    public FrontendCurrentRunnable(Queue<ServiceTask> frontEndTasks) {
        this.frontNormalTasks = frontEndTasks;
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
                task.getService().execute(task);
            }

            //threadUsageStat end
            if (workUsage != null) {
                workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
            }
        }
    }

}
