/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2018/7/12.
 */
public class BackEndRecycleRunnable implements Runnable, BackEndCleaner {

    private final MySQLConnection backendConnection;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condRelease = this.lock.newCondition();

    public BackEndRecycleRunnable(MySQLConnection backendConnection) {
        this.backendConnection = backendConnection;
        backendConnection.setRecycler(this);
    }


    @Override
    public void run() {
        try {
            lock.lock();
            try {
                if (backendConnection.isRunning()) {

                    if (!condRelease.await(10, TimeUnit.MILLISECONDS)) {
                        backendConnection.close("recycle time out");
                    } else {
                        backendConnection.release();
                    }
                } else {
                    backendConnection.release();
                }
            } catch (Exception e) {
                backendConnection.close("recycle exception");
            } finally {
                lock.unlock();
            }
        } catch (Throwable e) {
            backendConnection.close("recycle exception");
        }
    }


    public void singal() {
        lock.lock();
        try {
            condRelease.signal();
        } finally {
            lock.unlock();
        }
    }

}
