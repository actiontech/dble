/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
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

public class GetAndSyncDbInstanceKeyVariables implements Callable<KeyVariables> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAndSyncDbInstanceKeyVariables.class);
    private static final String COLUMN_LOWER_CASE = "@@lower_case_table_names";
    private static final String COLUMN_AUTOCOMMIT = "@@autocommit";
    private static final String COLUMN_READONLY = "@@read_only";
    private static final String COLUMN_MAX_PACKET = "@@max_allowed_packet";
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isFinish = false;
    private Condition finishCond = lock.newCondition();
    private volatile KeyVariables keyVariables;
    private PhysicalDbInstance ds;
    private final String columnIsolation;
    private final boolean needSync;

    public GetAndSyncDbInstanceKeyVariables(PhysicalDbInstance ds, boolean needSync) {
        this.ds = ds;
        this.needSync = needSync;
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
        String[] columns = new String[]{COLUMN_LOWER_CASE, COLUMN_AUTOCOMMIT, COLUMN_READONLY, COLUMN_MAX_PACKET, columnIsolation};
        StringBuilder sql = new StringBuilder("select ");
        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                sql.append(",");
            }
            sql.append(columns[i]);
        }
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(columns, new GetDbInstanceKeyVariablesListener());
        OneTimeConnJob sqlJob = new OneTimeConnJob(sql.toString(), null, resultHandler, ds);
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


    private class GetDbInstanceKeyVariablesListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (result.isSuccess()) {
                keyVariables = new KeyVariables();
                keyVariables.setLowerCase(!result.getResult().get(COLUMN_LOWER_CASE).equals("0"));

                keyVariables.setAutocommit(result.getResult().get(COLUMN_AUTOCOMMIT).equals("1"));
                keyVariables.setTargetAutocommit(SystemConfig.getInstance().getAutocommit() == 1);

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
                keyVariables.setTargetIsolation(SystemConfig.getInstance().getTxIsolation());
                keyVariables.setMaxPacketSize(Integer.parseInt(result.getResult().get(COLUMN_MAX_PACKET)));
                keyVariables.setTargetMaxPacketSize(SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE);
                keyVariables.setReadOnly(result.getResult().get(COLUMN_READONLY).equals("1"));

                if (needSync) {
                    boolean checkNeedSync = false;
                    if (keyVariables.getTargetMaxPacketSize() > keyVariables.getMaxPacketSize()) {
                        checkNeedSync = true;
                    }
                    if (keyVariables.isTargetAutocommit() != keyVariables.isAutocommit()) {
                        checkNeedSync = true;
                    } else {
                        ds.setAutocommitSynced(true);
                    }
                    if (keyVariables.getTargetIsolation() != keyVariables.getIsolation()) {
                        checkNeedSync = true;
                    } else {
                        ds.setIsolationSynced(true);
                    }

                    if (checkNeedSync) {
                        SyncDbInstanceKeyVariables task = new SyncDbInstanceKeyVariables(keyVariables, ds);
                        boolean synced = false;
                        try {
                            synced = task.call();
                        } catch (Exception e) {
                            LOGGER.warn("SyncDbInstanceKeyVariables error", e);
                        }
                        if (synced) {
                            ds.setAutocommitSynced(true);
                            ds.setIsolationSynced(true);
                            keyVariables.setMaxPacketSize(keyVariables.getTargetMaxPacketSize());
                        }
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
