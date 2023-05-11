package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.statistic.sql.StatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

public class RwTrackProbe extends AbstractTrackProbe {
    public static final Logger LOGGER = LoggerFactory.getLogger(RwTrackProbe.class);
    public final RwTraceResult rwTraceResult;

    final RWSplitNonBlockingSession currentSession;
    volatile boolean isTrace = false;

    public RwTrackProbe(RWSplitNonBlockingSession currentSession) {
        this.currentSession = currentSession;
        this.rwTraceResult = new RwTraceResult(currentSession);
    }

    public void setRequestTime() {
        isTrace = StatisticManager.getInstance().mainSwitch();
        sqlTracking(t -> t.setRequestTime(System.nanoTime(), System.currentTimeMillis()));
    }

    public void startProcess() {
        sqlTracking(t -> t.startProcess(System.nanoTime()));
    }

    public void setQuery(String sql, int sqlType) {
        sqlTracking(t -> t.setQuery(sql, sqlType));
    }

    public void setBackendRequestTime(MySQLResponseService service) {
        long requestTime = System.nanoTime();
        sqlTracking(t -> t.setBackendRequestTime(service, requestTime));
    }

    public void setBackendSqlAddRows(MySQLResponseService service) {
        sqlTracking(t -> t.setBackendSqlAddRows(service, null));
    }

    public void setBackendSqlSetRows(MySQLResponseService service, long rows) {
        sqlTracking(t -> t.setBackendSqlAddRows(service, rows));
    }

    public void setBackendResponseEndTime(MySQLResponseService service) {
        long time = System.nanoTime();
        sqlTracking(t -> t.setBackendResponseEndTime(service, time));
    }

    public void setBackendResponseClose(MySQLResponseService service) {
        sqlTracking(t -> t.setBackendResponseTxEnd(service, System.nanoTime()));
    }

    public void doSqlStat(long sqlRows, long netOutBytes, long resultSize) {
        sqlTracking(t -> t.setSqlStat(sqlRows, netOutBytes, resultSize));
    }

    public void setResponseTime(boolean isSuccess) {
        sqlTracking(t -> t.setResponseTime(isSuccess, System.nanoTime(), System.currentTimeMillis()));
    }

    public void setExit() {
        sqlTracking(t -> t.setExit());
    }

    private void sqlTracking(Consumer<RwTraceResult> consumer) {
        try {
            if (isTrace) {
                Optional.ofNullable(rwTraceResult).ifPresent(consumer);
            }
        } catch (Exception e) {
            // Should not affect the main task
            LOGGER.warn("sqlTracking occurred ", e);
        }
    }
}
