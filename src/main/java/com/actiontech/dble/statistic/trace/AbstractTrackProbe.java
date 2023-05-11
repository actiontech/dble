package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTrackProbe {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractTrackProbe.class);

    public void setRequestTime() {
    }

    public void startProcess() {
    }

    public void setQuery(String sql) {
    }

    public void endParse() {
    }

    public void endRoute(RouteResultset rrs) {
    }

    public void endComplexRoute() {
    }

    public void endComplexExecute() {
    }

    public void readyToDeliver() {
    }

    public void setPreExecuteEnd(TraceResult.SqlTraceType type) {
    }

    public void setSubQuery() {
    }

    public void setBackendRequestTime(MySQLResponseService service) {
    }

    public void setBackendResponseTime(MySQLResponseService service) {
    }

    public void startExecuteBackend() {
    }

    public void allBackendConnReceive() {
    }

    public void setBackendSqlAddRows(MySQLResponseService service) {
    }

    public void setBackendSqlSetRows(MySQLResponseService service, long rows) {
    }

    public void setBackendResponseEndTime(MySQLResponseService service) {
    }

    public void setBackendResponseTxEnd(MySQLResponseService service) {
    }

    public void setBackendResponseClose(MySQLResponseService service) {
    }

    public void doSqlStat(long sqlRows, long netOutBytes, long resultSize) {
    }

    public void setResponseTime(boolean isSuccess) {
    }

    public void setExit() {
    }

    public void setBeginCommitTime() {
    }

    public void setFinishedCommitTime() {
    }

    public void setHandlerStart(DMLResponseHandler handler) {
    }

    public void setHandlerEnd(DMLResponseHandler handler) {
    }

    public void setTraceBuilder(BaseHandlerBuilder baseBuilder) {
    }

    public void setTraceSimpleHandler(ResponseHandler simpleHandler) {
    }

    public void clear() {

    }
}
