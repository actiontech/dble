/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import java.util.Queue;
import java.util.concurrent.Executor;

public class FrontEndExecutor extends Thread {

    private final Executor executor;
    private final Queue<FrontendCommandHandler> frontHandlerQueue;

    public FrontEndExecutor(Executor executor, Queue frontHandlerQueue, int poolSize) {
        this.executor = executor;
        this.frontHandlerQueue = frontHandlerQueue;
    }

    @Override
    public void run() {
        FrontendCommandHandler handler;
        while (true) {
            while ((handler = frontHandlerQueue.poll()) != null) {
                final FrontendCommandHandler finalHandler = handler;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        finalHandler.handle();
                    }
                });
            }
        }
    }
}
