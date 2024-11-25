/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.handler;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2018/7/12.
 */
public class BackEndRecycleRunnable implements Runnable, BackEndCleaner {

    private final MySQLResponseService service;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condRelease = this.lock.newCondition();

    public BackEndRecycleRunnable(MySQLResponseService service) {
        this.service = service;
        service.setRecycler(this);
    }

    @Override
    public void run() {
        BackendConnection conn = service.getConnection();
        if (conn.isClosed()) {
            return;
        }
        boolean awaitTimeout = false;
        try {
            lock.lock();
            try {
                if (service.isRowDataFlowing()) {
                    if (!condRelease.await(SystemConfig.getInstance().getReleaseTimeout(), TimeUnit.MILLISECONDS)) {
                        awaitTimeout = true;
                    }
                }
            } catch (Exception e) {
                service.getConnection().businessClose("recycle exception");
            } finally {
                lock.unlock();
            }
            if (conn.isClosed()) {
                return;
            }
            if (awaitTimeout) {
                conn.businessClose("recycle time out");
            } else {
                service.release();
            }
        } catch (Throwable e) {
            service.getConnection().businessClose("recycle exception");
        }
    }


    public void signal() {
        lock.lock();
        try {
            service.setRowDataFlowing(false);
            condRelease.signal();
        } finally {
            lock.unlock();
        }

    }

}
