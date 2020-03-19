/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class GetAndSyncDataSourceKeyVariables implements Callable<KeyVariables> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAndSyncDataSourceKeyVariables.class);
    private static final String COLUMN_LOWER_CASE = "@@lower_case_table_names";
    private static final String COLUMN_AUTOCOMMIT = "@@autocommit";
    private static final String COLUMN_READONLY = "@@read_only";
    private static final String SQL = "select @@lower_case_table_names,@@autocommit, @@read_only,";
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isFinish = false;
    private Condition finishCond = lock.newCondition();
    private volatile KeyVariables keyVariables;
    private volatile boolean needSync = false;
    private PhysicalDataSource ds;
    private final String columnIsolation;

    public GetAndSyncDataSourceKeyVariables(PhysicalDataSource ds) {
        this.ds = ds;
        String isolationName = VersionUtil.getIsolationNameByVersion(ds.getDsVersion());
        if (isolationName != null) {
            columnIsolation = "@@" + isolationName;
        } else {
            columnIsolation = null;
        }
    }

    @Override
    public KeyVariables call() {
        if (columnIsolation == null) {
            return keyVariables;
        }
        String[] columns = new String[]{COLUMN_LOWER_CASE, COLUMN_AUTOCOMMIT, COLUMN_READONLY, columnIsolation};
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(columns, new GetDataSourceKeyVariablesListener());
        OneTimeConnJob sqlJob = new OneTimeConnJob(SQL + columnIsolation, null, resultHandler, ds);
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
        return keyVariables;
    }


    private class GetDataSourceKeyVariablesListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (result.isSuccess()) {
                keyVariables = new KeyVariables();
                keyVariables.setLowerCase(!result.getResult().get(COLUMN_LOWER_CASE).equals("0"));
                keyVariables.setAutocommit(result.getResult().get(COLUMN_AUTOCOMMIT).equals("1"));
                String isolation = result.getResult().get(columnIsolation);
                switch (isolation) {
                    case "READ-COMMITTED":
                        keyVariables.setIsolation(Isolations.READ_COMMITTED);
                        break;
                    case "READ-UNCOMMITTED":
                        keyVariables.setIsolation(Isolations.READ_UNCOMMITTED);
                        break;
                    case "REPEATABLE-READ":
                        keyVariables.setIsolation(Isolations.REPEATABLE_READ);
                        break;
                    case "SERIALIZABLE":
                        keyVariables.setIsolation(Isolations.SERIALIZABLE);
                        break;
                    default:
                        break;
                }
                boolean sysAutocommit = DbleServer.getInstance().getConfig().getSystem().getAutocommit() == 1;
                keyVariables.setTargetAutocommit(sysAutocommit);
                if (sysAutocommit != keyVariables.isAutocommit()) {
                    needSync = true;
                } else {
                    ds.setAutocommitSynced(true);
                }
                int sysTxIsolation = DbleServer.getInstance().getConfig().getSystem().getTxIsolation();
                keyVariables.setTargetIsolation(sysTxIsolation);
                if (sysTxIsolation != keyVariables.getIsolation()) {
                    needSync = true;
                } else {
                    ds.setIsolationSynced(true);
                }
                keyVariables.setReadOnly(result.getResult().get(COLUMN_READONLY).equals("1"));

                if (needSync) {
                    SyncDataSourceKeyVariables task = new SyncDataSourceKeyVariables(keyVariables, ds);
                    boolean synced = false;
                    try {
                        synced = task.call();
                    } catch (Exception e) {
                        LOGGER.warn("SyncDataSourceKeyVariables error", e);
                    }
                    if (synced) {
                        ds.setAutocommitSynced(true);
                        ds.setIsolationSynced(true);
                    }
                }
            } else {
                ds.setTestConnSuccess(false);
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
