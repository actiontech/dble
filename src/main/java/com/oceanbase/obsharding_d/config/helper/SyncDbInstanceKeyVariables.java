/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.helper;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.VersionUtil;
import com.oceanbase.obsharding_d.config.Isolations;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;
import com.oceanbase.obsharding_d.sqlengine.OneRawSQLQueryResultHandler;
import com.oceanbase.obsharding_d.sqlengine.OneTimeConnJob;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResult;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SyncDbInstanceKeyVariables implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncDbInstanceKeyVariables.class);
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isFinish = false;
    private Condition finishCond = lock.newCondition();
    private PhysicalDbInstance ds;
    private KeyVariables keyVariables;
    private volatile BoolPtr success = new BoolPtr(false);
    private final String isolationName;


    public SyncDbInstanceKeyVariables(KeyVariables keyVariables, PhysicalDbInstance ds) {
        this.keyVariables = keyVariables;
        this.ds = ds;
        isolationName = VersionUtil.getIsolationNameByVersion(ds.getDsVersion());
    }

    @Override
    public Boolean call() throws Exception {
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SyncDbInstanceKeyVariablesCallBack());
        String sql = genQuery();

        OneTimeConnJob sqlJob = new OneTimeConnJob(sql, null, resultHandler, ds);
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
        return success.get();
    }

    private String genQuery() {
        StringBuilder sql = new StringBuilder("set global ");
        boolean needAddComma = false;
        if (keyVariables.isTargetAutocommit() != keyVariables.isAutocommit()) {
            sql.append("autocommit=");
            if (keyVariables.isTargetAutocommit()) {
                sql.append("1");
            } else {
                sql.append("0");
            }
            needAddComma = true;
        }
        if (keyVariables.getTargetIsolation() != keyVariables.getIsolation()) {
            if (needAddComma) {
                sql.append(",");
            }
            sql.append(isolationName).append("='");
            switch (keyVariables.getTargetIsolation()) {
                case Isolations.READ_UNCOMMITTED:
                    sql.append("READ-UNCOMMITTED");
                    break;
                case Isolations.READ_COMMITTED:
                    sql.append("READ-COMMITTED");
                    break;
                case Isolations.REPEATABLE_READ:
                    sql.append("REPEATABLE-READ");
                    break;
                case Isolations.SERIALIZABLE:
                    sql.append("SERIALIZABLE");
                    break;
                default:
                    //will not happen
            }
            needAddComma = true;
            sql.append("'");
        }
        if (keyVariables.getTargetMaxPacketSize() > keyVariables.getMaxPacketSize()) {
            if (needAddComma) {
                sql.append(",");
            }
            sql.append("max_allowed_packet=");
            sql.append(keyVariables.getTargetMaxPacketSize());
            needAddComma = true; // used for feature
        }
        return sql.toString();
    }

    private class SyncDbInstanceKeyVariablesCallBack implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            success.set(result.isSuccess());
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
