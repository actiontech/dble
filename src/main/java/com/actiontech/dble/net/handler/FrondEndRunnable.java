/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import java.util.concurrent.BlockingQueue;

public class FrondEndRunnable implements Runnable {
    private final BlockingQueue<FrontendCommandHandler> frontHandlerQueue;

    public FrondEndRunnable(BlockingQueue frontHandlerQueue) {
        this.frontHandlerQueue = frontHandlerQueue;
    }

    @Override
    public void run() {
        FrontendCommandHandler handler;
        while (true) {
            try {
                handler = frontHandlerQueue.take();
                handler.handle();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
