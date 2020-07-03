package com.actiontech.dble.net.executor;

import com.actiontech.dble.net.service.ServiceTask;

import java.util.Queue;

/**
 * Created by szf on 2020/7/9.
 */
public class BackendCurrentRunnable implements Runnable {

    private final Queue<ServiceTask> concurrentBackQueue;

    public BackendCurrentRunnable(Queue<ServiceTask> concurrentBackQueue) {
        this.concurrentBackQueue = concurrentBackQueue;
    }

    @Override
    public void run() {
        ServiceTask task;
        while (true) {
            while ((task = concurrentBackQueue.poll()) != null) {
                task.getService().consumerInternalData();
            }
        }
    }
}
