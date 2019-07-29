/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import java.util.Queue;

public class ConcurrentBackEndHandlerRunnable implements Runnable {
    private final Queue<BackendAsyncHandler> backendHandlerQueue;

    public ConcurrentBackEndHandlerRunnable(Queue<BackendAsyncHandler> backendHandlerQueue) {
        this.backendHandlerQueue = backendHandlerQueue;

    }

    @Override
    public void run() {
        BackendAsyncHandler handler;
        while (true) {
            while ((handler = backendHandlerQueue.poll()) != null) {
                handler.executeQueue();
            }
        }
    }
}
