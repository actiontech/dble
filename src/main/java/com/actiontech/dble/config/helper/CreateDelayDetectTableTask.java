/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CreateDelayDetectTableTask extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDelayDetectTableTask.class);
    private final PhysicalDbInstance ds;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition finishCond = lock.newCondition();
    private boolean isFinish = false;
    private BoolPtr successFlag;

    public CreateDelayDetectTableTask(PhysicalDbInstance ds, BoolPtr successFlag) {
        this.ds = ds;
        this.successFlag = successFlag;
    }

    @Override
    public void run() {
        String table = ds.getDbGroupConfig().getDelayDatabase() + ".u_delay";

        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new DelayDetectionListener());
        String createTableSQL = "create table if not exists " + table +
                " (source VARCHAR(256) primary key,real_timestamp varchar(26) NOT NULL,logic_timestamp BIGINT default 0)";
        OneTimeConnJob sqlJob = new OneTimeConnJob(createTableSQL, null, resultHandler, ds);
        sqlJob.run();
        lock.lock();
        try {
            while (!isFinish) {
                finishCond.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("test conn Interrupted:", e);
        } finally {
            lock.unlock();
        }
    }

    private class DelayDetectionListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (result.isSuccess()) {
                successFlag.set(true);
            }
            handleFinished();
        }

        private void handleFinished() {
            lock.lock();
            try {
                isFinish = true;
                finishCond.signal();
            } finally {
                lock.unlock();
            }
        }


    }
}
