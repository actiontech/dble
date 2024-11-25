/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.stage;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.StageRecorder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionStage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public abstract class Stage implements TransactionStage {
    protected Lock lock;
    protected Condition done;
    protected boolean isSync = false;
    protected AtomicBoolean isFinished = new AtomicBoolean(false);
    protected StageRecorder stageRecorder;

    public void waitFinished() {
        lock.lock();
        try {
            while (!isFinished.get()) {
                done.await();
            }
        } catch (InterruptedException e) {
            // ignore
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void signalFinished() {
        lock.lock();
        try {
            isFinished.set(true);
            if (isFinished.get()) {
                done.signal();
            }
        } finally {
            lock.unlock();
        }
    }
}
