/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OwnThreadDMLHandler
 *
 * @author ActionTech
 * @CreateTime 2014/11/27
 */
public abstract class OwnThreadDMLHandler extends BaseDMLHandler {
    /* if the own thread need to terminated, true if own thread running */
    private AtomicBoolean ownJobFlag;
    private Object ownThreadLock = new Object();
    private boolean preparedToRecycle;

    public OwnThreadDMLHandler(long id, NonBlockingSession session) {
        super(id, session);
        this.ownJobFlag = new AtomicBoolean(false);
        this.preparedToRecycle = false;
    }

    @Override
    public final void onTerminate() throws Exception {
        if (ownJobFlag.compareAndSet(false, true)) {
            // terminated before the thread started
            recycleResources();
        } else { // thread started
            synchronized (ownThreadLock) {
                if (!preparedToRecycle) { // not ready to release resources
                    terminateThread();
                }
            }
        }
    }

    /**
     * @param objects
     */
    protected final void startOwnThread(final Object... objects) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                if (terminate.get())
                    return;
                if (ownJobFlag.compareAndSet(false, true)) {
                    try {
                        ownThreadJob(objects);
                    } finally {
                        synchronized (ownThreadLock) {
                            preparedToRecycle = true;
                        }
                        recycleResources();
                    }
                }
            }
        });
    }

    protected abstract void ownThreadJob(Object... objects);

    /* ending the running thread */
    protected abstract void terminateThread() throws Exception;

    /* after thread terminated */
    protected abstract void recycleResources();

}
