/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.config.ErrorCode.ER_YES;

/**
 * Created by szf on 2018/7/17.
 */
public class PauseEndThreadPool {

    protected static final Logger LOGGER = LoggerFactory.getLogger(PauseEndThreadPool.class);
    public static final String CONTINUE_TYPE_SINGLE = "1";
    public static final String CONTINUE_TYPE_MULTIPLE = "2";

    public final int pauseThreadNum = 10;

    public final AtomicInteger idleCount = new AtomicInteger(0);

    private final BlockingQueue<PauseTask> handlerQueue;

    private final ExecutorService executor = ExecutorUtil.createFixed("pauseBusinessExecutor", pauseThreadNum);

    private ReentrantLock queueLock = new ReentrantLock();

    private Condition condRelease = this.queueLock.newCondition();

    private volatile boolean teminateFlag = false;

    private final int timeout;

    private final AtomicInteger queueNumber;

    private final int queueLimit;

    public PauseEndThreadPool(int timeout, int queueLimit) {
        this.timeout = timeout;
        handlerQueue = new LinkedBlockingQueue();
        queueNumber = new AtomicInteger(0);
        this.queueLimit = queueLimit;
        if (queueLimit > 0) {
            for (int i = 0; i < pauseThreadNum; i++) {
                executor.execute(new PauseThreadRunnable());
            }
        }

    }


    //to put new task for this thread pool
    public boolean offer(ShardingService service, String nextStep, RouteResultset rrs) {
        PauseTask task = new PauseTask(rrs, nextStep, service);
        queueLock.lock();
        try {
            if (!teminateFlag) {
                if (queueNumber.incrementAndGet() <= queueLimit) {
                    handlerQueue.offer(task);
                    return true;
                } else {
                    service.writeErrMessage(ER_YES, "The node is pausing, wait list is full");
                    queueNumber.decrementAndGet();
                }
                return true;
            } else {
                return false;
            }
        } finally {
            queueLock.unlock();
        }
    }


    //delete the it self
    public void continueExec() {
        queueLock.lock();
        try {
            teminateFlag = true;
            condRelease.signalAll();
        } finally {
            queueLock.unlock();
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (!(pauseThreadNum == idleCount.intValue())) {
                    LockSupport.parkNanos(100);
                }
                executor.shutdownNow();
            }
        });


    }


    private class PauseThreadRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    PauseTask task = handlerQueue.take();

                    if (waitForPause(task)) {
                        //execute the rrs from the session
                        switch (task.getNextStep()) {
                            case CONTINUE_TYPE_SINGLE:
                                task.getService().getSession2().execute(task.getRrs());
                                break;
                            case CONTINUE_TYPE_MULTIPLE:
                                task.getService().getSession2().executeMultiSelect(task.getRrs());
                                break;
                            default:
                                break;
                        }
                    }
                    if (teminateFlag && handlerQueue.size() == 0) {
                        break;
                    }

                } catch (Exception e) {
                    LOGGER.info("the pause end thread with error", e);
                }
            }
            idleCount.getAndIncrement();

        }


        public boolean waitForPause(PauseTask task) {
            queueLock.lock();
            try {
                if (!teminateFlag) {
                    long wait = task.waitTime();
                    if (wait > 0) {
                        boolean signal = condRelease.await(wait, TimeUnit.MILLISECONDS);
                        if (!signal) {
                            task.timeOut();
                            return false;
                        }
                    } else if (!teminateFlag) {
                        task.timeOut();
                        return false;
                    }
                }
            } catch (Exception e) {
                LOGGER.info("the pause end thread with error", e);
            } finally {
                queueLock.unlock();
            }
            return true;
        }
    }

    private class PauseTask {

        RouteResultset rrs = null;
        String nextStep = null;
        ShardingService service = null;
        long timestamp;


        PauseTask(RouteResultset rrs, String nextStep, ShardingService service) {
            this.nextStep = nextStep;
            this.rrs = rrs;
            this.service = service;
            timestamp = System.currentTimeMillis();
        }

        void timeOut() {
            queueNumber.decrementAndGet();
            service.writeErrMessage(ER_YES, "waiting time exceeded wait_limit from pause shardingNode");
        }

        long waitTime() {
            return timestamp + timeout - System.currentTimeMillis();
        }


        public RouteResultset getRrs() {
            return rrs;
        }

        public void setRrs(RouteResultset rrs) {
            this.rrs = rrs;
        }

        public String getNextStep() {
            return nextStep;
        }

        public void setNextStep(String nextStep) {
            this.nextStep = nextStep;
        }


        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }


        public ShardingService getService() {
            return service;
        }

        public void setService(ShardingService service) {
            this.service = service;
        }

    }
}



