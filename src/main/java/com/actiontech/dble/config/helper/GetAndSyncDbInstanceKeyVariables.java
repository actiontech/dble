/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class GetAndSyncDbInstanceKeyVariables implements Callable<KeyVariables> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAndSyncDbInstanceKeyVariables.class);
    private static final String COLUMN_LOWER_CASE = "@@lower_case_table_names";
    private static final String COLUMN_AUTOCOMMIT = "@@autocommit";
    private static final String COLUMN_READONLY = "@@read_only";
    private static final String COLUMN_MAX_PACKET = "@@max_allowed_packet";
    private static final String COLUMN_VERSION = "@@version";
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isFinish = false;
    private Condition finishCond = lock.newCondition();
    private volatile KeyVariables keyVariables;
    private PhysicalDbInstance ds;
    private final String columnIsolation;
    private final boolean needSync;
    private volatile boolean fail = false;

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
        String[] columns = new String[]{COLUMN_LOWER_CASE, COLUMN_AUTOCOMMIT, COLUMN_READONLY, COLUMN_MAX_PACKET, columnIsolation, COLUMN_VERSION};
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
                boolean await = finishCond.await(ds.getConfig().getPoolConfig().getHeartbeatPeriodMillis(), TimeUnit.MILLISECONDS);
                if (!await) {
                    if (!isFinish) {
                        fail = true;
                        isFinish = true;
                        LOGGER.warn("test conn timeout,TCP connection may be lost");
                    }
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("test conn Interrupted:", e);
        } finally {
            lock.unlock();
            ReloadLogHelper.debug("get key variables :{},dbInstance:{},result:{}", LOGGER, sql.toString(), ds, keyVariables);
        }
        return keyVariables;
    }


    private class GetDbInstanceKeyVariablesListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            boolean isSuccess = result.isSuccess();
            ds.setTestConnSuccess(isSuccess);
            if (isFinish) {
                if (fail)
                    LOGGER.info("test conn timeout, need to resynchronize");
                return;
            }
            KeyVariables keyVariablesTmp = null;
            if (isSuccess) {
                keyVariablesTmp = new KeyVariables();
                keyVariablesTmp.setLowerCase(!result.getResult().get(COLUMN_LOWER_CASE).equals("0"));

                keyVariablesTmp.setAutocommit(result.getResult().get(COLUMN_AUTOCOMMIT).equals("1"));
                keyVariablesTmp.setTargetAutocommit(SystemConfig.getInstance().getAutocommit() == 1);

                String isolation = result.getResult().get(columnIsolation);
                switch (isolation) {
                    case "READ-COMMITTED":
                        keyVariablesTmp.setIsolation(Isolations.READ_COMMITTED);
                        break;
                    case "READ-UNCOMMITTED":
                        keyVariablesTmp.setIsolation(Isolations.READ_UNCOMMITTED);
                        break;
                    case "REPEATABLE-READ":
                        keyVariablesTmp.setIsolation(Isolations.REPEATABLE_READ);
                        break;
                    case "SERIALIZABLE":
                        keyVariablesTmp.setIsolation(Isolations.SERIALIZABLE);
                        break;
                    default:
                        break;
                }
                keyVariablesTmp.setTargetIsolation(SystemConfig.getInstance().getTxIsolation());
                keyVariablesTmp.setMaxPacketSize(Integer.parseInt(result.getResult().get(COLUMN_MAX_PACKET)));
                keyVariablesTmp.setTargetMaxPacketSize(SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE);
                keyVariablesTmp.setReadOnly(result.getResult().get(COLUMN_READONLY).equals("1"));
                keyVariablesTmp.setVersion(result.getResult().get(COLUMN_VERSION));

                if (needSync) {
                    boolean checkNeedSync = false;
                    if (keyVariablesTmp.getTargetMaxPacketSize() > keyVariablesTmp.getMaxPacketSize()) {
                        checkNeedSync = true;
                    }
                    if (keyVariablesTmp.isTargetAutocommit() != keyVariablesTmp.isAutocommit()) {
                        checkNeedSync = true;
                    } else {
                        ds.setAutocommitSynced(true);
                    }
                    if (keyVariablesTmp.getTargetIsolation() != keyVariablesTmp.getIsolation()) {
                        checkNeedSync = true;
                    } else {
                        ds.setIsolationSynced(true);
                    }

                    if (checkNeedSync) {
                        SyncDbInstanceKeyVariables task = new SyncDbInstanceKeyVariables(keyVariablesTmp, ds);
                        boolean synced = false;
                        try {
                            synced = task.call();
                        } catch (Exception e) {
                            LOGGER.warn("SyncDbInstanceKeyVariables error", e);
                        }

                        if (synced) {
                            ds.setAutocommitSynced(true);
                            ds.setIsolationSynced(true);
                            keyVariablesTmp.setMaxPacketSize(keyVariablesTmp.getTargetMaxPacketSize());
                        }
                    }
                }
            }
            handleFinished(keyVariablesTmp);
        }

        private void handleFinished(KeyVariables keyVariablesTmp) {
            lock.lock();
            try {
                if (isFinish) {
                    if (fail)
                        LOGGER.info("test conn timeout, need to resynchronize");
                } else {
                    keyVariables = keyVariablesTmp;
                    isFinish = true;
                }
                finishCond.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
