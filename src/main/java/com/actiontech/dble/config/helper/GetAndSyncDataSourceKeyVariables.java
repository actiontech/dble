/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
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
    private static final String[] COLUMNS = new String[]{"@@lower_case_table_names", "@@autocommit", "@@tx_isolation", "@@read_only"};
    private static final String SQL = "select @@lower_case_table_names,@@autocommit, @@tx_isolation,@@read_only";
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isFinish = false;
    private Condition finishCond = lock.newCondition();
    private volatile KeyVariables keyVariables;
    private volatile boolean needSync = false;
    private PhysicalDatasource ds;

    public GetAndSyncDataSourceKeyVariables(PhysicalDatasource ds) {
        this.ds = ds;
    }

    @Override
    public KeyVariables call() {
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(COLUMNS, new GetDataSourceKeyVariablesListener());
        OneTimeConnJob sqlJob = new OneTimeConnJob(SQL, null, resultHandler, ds);
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
                keyVariables.setLowerCase(!result.getResult().get(COLUMNS[0]).equals("0"));
                keyVariables.setAutocommit(result.getResult().get(COLUMNS[1]).equals("1"));
                String isolation = result.getResult().get(COLUMNS[2]);
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
                // todo:keyVariables.isReadOnly();
                keyVariables.setReadOnly(result.getResult().get(COLUMNS[3]).equals("1"));

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
