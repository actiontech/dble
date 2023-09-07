package com.actiontech.dble.singleton;

import com.actiontech.dble.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ThreadCheckerScheduler {
    public static final Logger LOGGER = LoggerFactory.getLogger("ThreadChecker");
    private static final ThreadCheckerScheduler INSTANCE = new ThreadCheckerScheduler();
    private ScheduledExecutorService scheduledExecutor;

    public ThreadCheckerScheduler() {
        this.scheduledExecutor = ExecutorUtil.createFixedScheduled("ThreadChecker", 1, null);
    }

    public void init() {
        scheduledExecutor.scheduleAtFixedRate(checkThread(), 0L, 2, TimeUnit.MINUTES);
    }

    private Runnable checkThread() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    ThreadChecker.getInstance().doSelfCheck();
                } catch (Throwable e) {
                    LOGGER.warn("doSelfCheck() happen fail, exception :", e);
                }
            }
        };
    }

    public static ThreadCheckerScheduler getInstance() {
        return INSTANCE;
    }
}
