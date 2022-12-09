/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.model.SystemConfig;

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
        if (backendConnection.isClosed()) {
            return;
        }
        boolean awaitTimeout = false;

        try {
            lock.lock();
            try {
                if (backendConnection.isRowDataFlowing()) {
                    SystemConfig systemConfig = DbleServer.getInstance().getConfig().getSystem();
                    if (!condRelease.await(systemConfig.getReleaseTimeout(), TimeUnit.MILLISECONDS)) {
                        awaitTimeout = true;
                    }
                }
            } catch (Exception e) {
                backendConnection.close("recycle exception");
            } finally {
                lock.unlock();
            }
            if (backendConnection.isClosed()) {
                return;
            }
            if (awaitTimeout) {
                backendConnection.close("recycle time out");
            } else {
                backendConnection.release();
            }
        } catch (Throwable e) {
            backendConnection.close("recycle exception");
        }
    }


    public void signal() {
        lock.lock();
        try {
            backendConnection.setRowDataFlowing(false);
            condRelease.signal();
        } finally {
            lock.unlock();
        }

    }

}
