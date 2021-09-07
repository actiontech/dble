package com.actiontech.dble.net.executor;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.BlockingDeque;

/**
 * Created by szf on 2020/6/18.
 */
public class FrontendBlockRunnable implements FrontendRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendBlockRunnable.class);
    private final BlockingDeque<ServiceTask> frontNormalTasks;
    private final ThreadContext threadContext = new ThreadContext();

    public FrontendBlockRunnable(BlockingDeque<ServiceTask> frontEndTasks) {
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
                    LOGGER.debug("interrupt thread:{},frontNormalTasks:{}", Thread.currentThread().toString(), frontNormalTasks);
                    break;
                }
                task = frontNormalTasks.take();

                if (task.getService() == null) {
                    continue;
                }

                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }
                //handler data
                task.getService().execute(task, threadContext);

                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (InterruptedException e) {
                DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                LOGGER.debug("interrupt thread:{},frontNormalTasks:{}", Thread.currentThread().toString(), frontNormalTasks, e);
                break;
            } catch (Throwable e) {
                LOGGER.error("process task error", e);
            }
        }
    }


}
