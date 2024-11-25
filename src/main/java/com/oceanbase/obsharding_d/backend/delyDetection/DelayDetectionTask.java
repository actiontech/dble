/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.delyDetection;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.sqlengine.OneRawSQLQueryResultHandler;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResult;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResultListener;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class DelayDetectionTask {
    private DelayDetection delayDetection;
    private DelayDetectionSqlJob sqlJob;
    private static final String LOGIC_TIMESTAMP = "logic_timestamp";
    private AtomicBoolean quit = new AtomicBoolean(false);
    private static final String[] MYSQL_DELAY_DETECTION_COLS = new String[]{
            "logic_timestamp",
    };

    public DelayDetectionTask(DelayDetection delayDetection) {
        this.delayDetection = delayDetection;
    }

    public void execute() {
        delayDetection.updateLastSendQryTime();
        if (Objects.isNull(sqlJob)) {
            String[] fetchCols = {};
            if (delayDetection.getSource().isReadInstance()) {
                fetchCols = MYSQL_DELAY_DETECTION_COLS;
            }
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(fetchCols, new DelayDetectionListener());
            sqlJob = new DelayDetectionSqlJob(delayDetection, resultHandler);
            sqlJob.setVersionVal(delayDetection.getVersion().incrementAndGet());
            delayDetection.getSource().createConnectionSkipPool(null, sqlJob);
        } else {
            sqlJob.execute();
        }
    }

    public boolean isQuit() {
        return quit.get();
    }

    public void close() {
        if (quit.compareAndSet(false, true)) {
            sqlJob = null;
        }
    }

    private class DelayDetectionListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            delayDetection.updateLastReceivedQryTime();
            if (!result.isSuccess()) {
                return;
            }
            PhysicalDbInstance source = delayDetection.getSource();
            if (source.isReadInstance()) {
                Map<String, String> resultResult = result.getResult();
                String logicTimestamp = Optional.ofNullable(resultResult.get(LOGIC_TIMESTAMP)).orElse("0");
                long logic = Long.parseLong(logicTimestamp);
                delayDetection.delayCal(logic);
            } else {
                delayDetection.setResult(DelayDetectionStatus.OK);
            }
            //after the CREATE statement is successfully executed, the update statement needs to be executed
            if (!delayDetection.isTableExists()) {
                delayDetection.setTableExists(true);
                delayDetection.execute();
            }
        }
    }
}
