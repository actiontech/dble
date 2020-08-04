/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.handler;

import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2018/11/6.
 */
public class BackEndDataCleaner implements BackEndCleaner {
    public static final Logger LOGGER = LoggerFactory.getLogger(BackEndDataCleaner.class);
    private final MySQLResponseService service;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condRelease = this.lock.newCondition();

    public BackEndDataCleaner(MySQLResponseService service) {
        this.service = service;
        service.setRecycler(this);
    }

    public void waitUntilDataFinish() {
        lock.lock();
        try {
            while (service.isRowDataFlowing() && !service.getConnection().isClosed()) {
                LOGGER.info("await for the row data get to a end");
                condRelease.await();
            }
        } catch (Throwable e) {
            LOGGER.warn("get error when waitUntilDataFinish, the connection maybe polluted");
        } finally {
            lock.unlock();
        }
    }


    public void signal() {
        lock.lock();
        try {
            condRelease.signal();
        } finally {
            lock.unlock();
        }
    }
}
