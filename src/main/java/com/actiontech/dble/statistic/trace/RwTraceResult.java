package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.entry.BackendInfo;
import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class RwTraceResult implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RwTraceResult.class);
    protected long requestStart;
    protected long requestStartMs;
    protected long parseStart; // requestEnd
    protected long requestEnd;
    protected long requestEndMs;
    protected String sql;
    protected String schema;
    protected long sqlRows;
    protected volatile long examinedRows;
    protected long netOutBytes;
    protected long resultSize;
    protected FrontendInfo frontendInfo;
    final RWSplitNonBlockingSession currentSession;
    protected List<ActualRoute> actualRouteList = Lists.newCopyOnWriteArrayList();
    protected volatile long previousTxId = 0;


    public RwTraceResult(RWSplitNonBlockingSession currentSession) {
        this.currentSession = currentSession;
        this.frontendInfo = new FrontendInfo(currentSession.getService());
    }

    public void setRequestTime(long time, long timeMs) {
        reset();
        this.requestStart = time;
        this.requestStartMs = timeMs;
    }

    public void startProcess(long time) {
        this.parseStart = time;
    }

    public void setQuery(String sql0) {
        this.schema = currentSession.getService().getSchema();
        this.sql = sql0;
        // multi-query
        if (currentSession.getIsMultiStatement().get() && currentSession.getMultiQueryHandler() != null) {
            Optional<ActualRoute> find = actualRouteList.stream().filter(f -> (f.handler == currentSession.getMultiQueryHandler())).findFirst();
            if (find.isPresent()) {
                ActualRoute ar = find.get();
                ar.setSql(sql0);
                ar.setRow(0);
                ar.setFinished(0);
                this.requestEnd = 0;
            }
            examinedRows = 0;
        }
    }

    public void setBackendRequestTime(MySQLResponseService service, long time) {
        final ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            Optional<ActualRoute> find = actualRouteList.stream().filter(f -> (f.handler == responseHandler)).findFirst();
            if (!find.isPresent()) {
                ActualRoute ar = new ActualRoute(responseHandler, sql, time);
                actualRouteList.add(ar);
            }
        }
    }


    public void setBackendSqlAddRows(MySQLResponseService service, Long num) {
        final ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            Optional<ActualRoute> find = actualRouteList.stream().filter(f -> (f.handler == responseHandler)).findFirst();
            if (find.isPresent()) {
                ActualRoute ar = find.get();
                if (num == null) {
                    ar.addRow();
                } else {
                    ar.setRow(num);
                }
            }
        }
    }

    public void setBackendResponseEndTime(MySQLResponseService service, long time) {
        ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            Optional<ActualRoute> find = actualRouteList.stream().filter(f -> (f.handler == responseHandler && f.finished == 0)).findFirst();
            if (find.isPresent()) {
                ActualRoute ar = find.get();
                ar.setFinished(time);

                examinedRows += ar.getRow();
                StatisticBackendSqlEntry bEntry = new StatisticBackendSqlEntry(
                        frontendInfo,
                        new BackendInfo(service.getConnection(), "-"),
                        ar.getRequestTime(), ar.getSql(), -99, ar.getRow(), ar.getFinished());
                bEntry.setNeedToTx(isNeedToTx());
                StatisticManager.getInstance().push(bEntry);
            }
        }
    }

    public void setBackendResponseTxEnd(MySQLResponseService service, long time) {
        if (!isNeedToTx()) {
            StatisticBackendSqlEntry bEntry = new StatisticBackendSqlEntry(
                    new FrontendInfo(currentSession.getService()),
                    new BackendInfo(service.getConnection(), "-"),
                    time, "/** txEnd **/", 0, 0, time);
            bEntry.setNeedToTx(true);
            StatisticManager.getInstance().push(bEntry);
        }
    }

    public void setSqlStat(long sqlRows0, long netOutBytes0, long resultSize0) {
        this.sqlRows = sqlRows0;
        this.netOutBytes = netOutBytes0;
        this.resultSize = resultSize0;
    }

    public void setResponseTime(boolean isSuccess, long time, long timeMs) {
        if (this.requestEnd == 0 || requestEndMs == 0) {
            this.requestEnd = time;
            this.requestEndMs = timeMs;
            if (this.isCompletedV1() && isSuccess) {
                StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(frontendInfo, requestStart, requestStartMs,
                        schema, sql, currentSession.getService().getTxId(), examinedRows, sqlRows,
                        netOutBytes, resultSize, requestEnd, requestEndMs);
                StatisticManager.getInstance().push(f);
            }
        }
    }

    public void setExit(long time) {
        long timeMs = System.currentTimeMillis();
        StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(frontendInfo, time, timeMs,
                null, "exit", currentSession.getService().getTxId(), 0, 0,
                0, 0, time, timeMs);
        StatisticManager.getInstance().push(f);
        reset();
        frontendInfo = null;
    }

    public boolean isNeedToTx() {
        /**
         * 1、begin;begin;
         * 2、begin;commit;
         * 3、set autocommit =0; begin;commit;ddl;set autocommit =;
         */
        if (previousTxId != currentSession.getService().getTxId()) {
            previousTxId = currentSession.getService().getTxId();
            return true;
        }
        return false;
    }

    public boolean isCompletedV1() {
        return sql != null && this.requestStart != 0 && this.requestEnd != 0;
    }

    private void reset() {
        parseStart = 0;
        requestEnd = 0;
        requestEndMs = 0;
        sql = null;
        schema = null;
        sqlRows = 0;
        examinedRows = 0;
        netOutBytes = 0;
        resultSize = 0;
        actualRouteList.clear();
    }

    @Override
    public RwTraceResult clone() {
        RwTraceResult rwTr;
        try {
            rwTr = (RwTraceResult) super.clone();
        } catch (Exception e) {
            LOGGER.warn("clone RwTraceResult error", e);
            throw new AssertionError(e.getMessage());
        }
        return rwTr;
    }

    protected static class ActualRoute { // backend execute
        ResponseHandler handler;
        String sql;
        long requestTime;
        long finished;
        long row;

        public ActualRoute(ResponseHandler handler, String sql, long requestTime) {
            this.handler = handler;
            this.sql = sql;
            this.requestTime = requestTime;
        }

        public long getRequestTime() {
            return requestTime;
        }

        public long getFinished() {
            return finished;
        }

        public void setFinished(long finished) {
            this.finished = finished;
        }

        public void addRow() {
            this.row += 1;
        }

        public void setRow(long row) {
            this.row = row;
        }

        public long getRow() {
            return row;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public void reset() {
            finished = 0;
            row = 0;
        }
    }
}
