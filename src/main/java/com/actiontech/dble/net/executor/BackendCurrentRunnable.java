package com.actiontech.dble.net.executor;

import com.actiontech.dble.net.service.ServiceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

/**
 * Created by szf on 2020/7/9.
 */
public class BackendCurrentRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendCurrentRunnable.class);

    private final Queue<ServiceTask> concurrentBackQueue;

    public BackendCurrentRunnable(Queue<ServiceTask> concurrentBackQueue) {
        this.concurrentBackQueue = concurrentBackQueue;
    }

    @Override
    public void run() {
        ServiceTask task;
        while (true) {
            try {
                while ((task = concurrentBackQueue.poll()) != null) {
                    task.getService().execute(task);
                }
            } catch (Throwable t) {
                LOGGER.warn("Unknown error:", t);
            }
        }
    }
}
