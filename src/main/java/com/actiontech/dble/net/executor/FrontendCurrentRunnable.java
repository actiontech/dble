package com.actiontech.dble.net.executor;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;

/**
 * Created by szf on 2020/7/9.
 */
public class FrontendCurrentRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendCurrentRunnable.class);

    private final Deque<ServiceTask> frontNormalTasks;

    public FrontendCurrentRunnable(Deque<ServiceTask> frontEndTasks) {
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
            if (Thread.currentThread().isInterrupted()) {
                DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
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
                task.getService().execute(task);
            }

            //threadUsageStat end
            if (workUsage != null) {
                workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
            }
        }
    }
}
