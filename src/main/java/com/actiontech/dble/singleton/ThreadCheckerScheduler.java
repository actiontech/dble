package com.actiontech.dble.singleton;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ThreadCheckerScheduler {
    public static final Logger LOGGER = LoggerFactory.getLogger(ThreadCheckerScheduler.class);
    private static final ThreadCheckerScheduler INSTANCE = new ThreadCheckerScheduler();
    private ScheduledExecutorService scheduledExecutor;

    public ThreadCheckerScheduler() {
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("ThreadChecker-%d").
                setUncaughtExceptionHandler((Thread threads, Throwable e) -> LOGGER.warn("unknown exception ", e)).build());
    }

    public void init() {
        scheduledExecutor.scheduleAtFixedRate(checkThread(), 0L, 2, TimeUnit.MINUTES);
    }

    private Runnable checkThread() {
        return new Runnable() {
            @Override
            public void run() {
                ThreadChecker.getInstance().doSelfCheck();
            }
        };
    }

    public static ThreadCheckerScheduler getInstance() {
        return INSTANCE;
    }
}
