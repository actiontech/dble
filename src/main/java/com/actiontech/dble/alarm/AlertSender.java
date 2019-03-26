package com.actiontech.dble.alarm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Created by szf on 2019/3/22.
 */
public class AlertSender implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertSender.class);

    private final BlockingQueue<AlertTask> alertQueue;

    public AlertSender(BlockingQueue<AlertTask> alertQueue) {
        this.alertQueue = alertQueue;
    }

    @Override
    public void run() {
        AlertTask alertTask;
        while (true) {
            try {
                alertTask = alertQueue.take();
                alertTask.send();
            } catch (Throwable e) {
                LOGGER.error("get error when send queue", e);
            }
        }
    }
}
