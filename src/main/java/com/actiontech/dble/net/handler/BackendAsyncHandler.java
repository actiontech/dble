/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mycat
 */
public abstract class BackendAsyncHandler implements NIOHandler {
    protected final ConcurrentLinkedQueue<byte[]> dataQueue = new ConcurrentLinkedQueue<>();
    protected final AtomicBoolean isHandling = new AtomicBoolean(false);

    protected void offerData(byte[] data, Executor executor) {
        if (dataQueue.offer(data)) {
            handleQueue(executor);
        } else {
            offerDataError();
        }
    }

    protected void offerData(byte[] data) {
        if (dataQueue.offer(data)) {
            pushTask();
        } else {
            offerDataError();
        }
    }

    private void pushTask() {
        if (isHandling.compareAndSet(false, true)) {
            DbleServer.getInstance().getBackHandlerQueue().offer(this);
        }
    }

    protected void handleQueue(final Executor executor) {
        if (isHandling.compareAndSet(false, true)) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleInnerData();
                    } catch (Exception e) {
                        handleDataError(e);
                    } finally {
                        isHandling.set(false);
                        if (dataQueue.size() > 0) {
                            handleQueue(executor);
                        }
                    }
                }
            });
        }
    }


    void executeQueue() {
        try {
            handleInnerData();
        } catch (Exception e) {
            handleDataError(e);
        } finally {
            isHandling.set(false);
            if (dataQueue.size() > 0) {
                pushTask();
            }
        }
    }

    private void handleInnerData() {
        byte[] data;

        //threadUsageStat start
        boolean useThreadUsageStat = false;
        String threadName = null;
        ThreadWorkUsage workUsage = null;
        long workStart = 0;
        if (DbleServer.getInstance().getConfig().getSystem().getUseThreadUsageStat() == 1) {
            useThreadUsageStat = true;
            threadName = Thread.currentThread().getName();
            workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);
            if (threadName.startsWith("backend")) {
                if (workUsage == null) {
                    workUsage = new ThreadWorkUsage();
                    DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                }
            }

            workStart = System.nanoTime();
        }
        //handleData
        while ((data = dataQueue.poll()) != null) {
            handleData(data);
        }
        //threadUsageStat end
        if (useThreadUsageStat && threadName.startsWith("backend")) {
            workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
        }
    }

    protected abstract void offerDataError();

    protected abstract void handleData(byte[] data);

    protected abstract void handleDataError(Exception e);
}
