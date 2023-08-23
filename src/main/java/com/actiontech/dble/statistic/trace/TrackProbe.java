package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.SessionStage;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.stat.QueryTimeCost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TrackProbe extends AbstractTrackProbe {
    public static final Logger LOGGER = LoggerFactory.getLogger(TrackProbe.class);
    private final NonBlockingSession currentSession;
    private volatile SessionStage sessionStage = SessionStage.Init;

    public final TraceResult traceResult;
    private volatile boolean isTrace = false;

    private final QueryTimeCost queryTimeCost;
    private volatile boolean timeCost = false;

    public TrackProbe(NonBlockingSession currentSession) {
        this.currentSession = currentSession;
        this.traceResult = new TraceResult(currentSession);
        this.queryTimeCost = new QueryTimeCost(currentSession.getShardingService().getConnection().getId());
    }

    // receives package (before being pushed into frontHandlerQueue)
    public void setRequestTime() {
        sessionStage = SessionStage.Read_SQL;
        long requestTime = System.nanoTime();
        isTrace = currentSession.isTraceEnable() || SlowQueryLog.getInstance().isEnableSlowLog() || StatisticManager.getInstance().mainSwitch();
        if (isTrace)
            traceResult.setRequestTime(requestTime, System.currentTimeMillis());
        timeCost = (SystemConfig.getInstance().getUseCostTimeStat() != 0) && !(ThreadLocalRandom.current().nextInt(100) >= SystemConfig.getInstance().getCostSamplePercent());
        if (timeCost)
            queryTimeCost.setRequestTime(requestTime);
    }

    public void startProcess() {
        sessionStage = SessionStage.Parse_SQL;
        long startProcess = System.nanoTime();
        if (isTrace)
            traceResult.startProcess(startProcess);
        if (timeCost)
            queryTimeCost.startProcess();
    }

    public void setQuery(String sql, int sqlType) {
        if (isTrace)
            traceResult.setQuery(sql, sqlType);
    }

    public void addTable(List<Pair<String, String>> tables) {
        if (isTrace)
            traceResult.addTable(tables);
    }

    public void endParse() {
        sessionStage = SessionStage.Route_Calculation;
        if (isTrace)
            traceResult.endParse(System.nanoTime());
        if (timeCost)
            queryTimeCost.endParse();
    }

    public void endRoute(RouteResultset rrs) {
        sessionStage = SessionStage.Prepare_to_Push;
        if (isTrace)
            traceResult.endRoute(System.nanoTime());
        if (timeCost)
            queryTimeCost.endRoute(rrs);
    }

    public void endComplexRoute() {
        if (timeCost)
            queryTimeCost.endComplexRoute();
    }

    public void endComplexExecute() {
        if (timeCost)
            queryTimeCost.endComplexExecute();
    }

    public void readyToDeliver() {
        if (timeCost)
            queryTimeCost.readyToDeliver();
    }

    public void setPreExecuteEnd(TraceResult.SqlTraceType type) {
        sessionStage = SessionStage.Execute_SQL;
        if (isTrace)
            traceResult.setPreExecuteEnd(type, System.nanoTime());
    }

    public void setSubQuery() {
        if (isTrace)
            traceResult.setSubQuery();
    }

    public void setBackendRequestTime(MySQLResponseService service) {
        long requestTime0 = System.nanoTime();
        if (isTrace)
            traceResult.setBackendRequestTime(service, requestTime0);
        if (timeCost)
            queryTimeCost.setBackendRequestTime(service.getConnection().getId(), requestTime0);
    }

    // receives the response package (before being pushed into BackendService.taskQueue)
    public void setBackendResponseTime(MySQLResponseService service) {
        sessionStage = SessionStage.Fetching_Result;
        long responseTime = System.nanoTime();
        if (isTrace)
            traceResult.setBackendResponseTime(service, responseTime);
        if (timeCost)
            queryTimeCost.setBackendResponseTime(service.getConnection().getId(), responseTime);
    }

    // start processing the response package (the first package taken out of the BackendService.taskQueue)
    public void startExecuteBackend() {
        if (timeCost)
            queryTimeCost.startExecuteBackend();
    }

    // When multiple nodes are queried, all nodes return the point in time of the EOF package
    public void allBackendConnReceive() {
        if (timeCost)
            queryTimeCost.allBackendConnReceive();
    }

    public void setBackendSqlAddRows(MySQLResponseService service) {
        if (isTrace)
            traceResult.setBackendSqlAddRows(service, null);
    }

    public void setBackendSqlSetRows(MySQLResponseService service, long rows) {
        if (isTrace)
            traceResult.setBackendSqlAddRows(service, rows);
    }

    // the final response package received,(include connection is accidentally closed or released)
    public void setBackendResponseEndTime(MySQLResponseService service) {
        sessionStage = SessionStage.First_Node_Fetched_Result;
        if (isTrace)
            traceResult.setBackendResponseEndTime(service, System.nanoTime());
        if (timeCost)
            queryTimeCost.setBackendResponseEndTime();
    }

    public void setBackendTerminateByComplex(MultiNodeMergeHandler mergeHandler) {
        if (isTrace)
            traceResult.setBackendTerminateByComplex(mergeHandler, System.nanoTime());
    }

    public void setBackendResponseTxEnd(MySQLResponseService service) {
        if (isTrace)
            traceResult.setBackendResponseTxEnd(service, System.nanoTime());
    }

    public void setBackendResponseClose(MySQLResponseService service) {
        if (isTrace)
            traceResult.setBackendResponseTxEnd(service, System.nanoTime());
    }

    public void setFrontendAddRows() {
        if (isTrace)
            traceResult.setFrontendAddRows();
    }

    public void setFrontendSetRows(long rows) {
        if (isTrace)
            traceResult.setFrontendSetRows(rows);
    }

    // get the rows、 netOutBytes、resultSize information in the last handler
    public void doSqlStat(long sqlRows, long netOutBytes, long resultSize) {
        if (isTrace)
            traceResult.setSqlStat(sqlRows, netOutBytes, resultSize);
    }

    public void setResponseTime(boolean isSuccess) {
        sessionStage = SessionStage.Finished;
        long responseTime = System.nanoTime();
        if (isTrace)
            traceResult.setResponseTime(isSuccess, responseTime, System.currentTimeMillis());
        if (timeCost)
            queryTimeCost.setResponseTime(responseTime);
    }

    public void setExit() {
        if (isTrace)
            traceResult.setExit();
    }

    public void setBeginCommitTime() {
        sessionStage = SessionStage.Distributed_Transaction_Commit;
        if (isTrace)
            traceResult.setAdtCommitBegin(System.nanoTime());
    }

    public void setFinishedCommitTime() {
        if (isTrace)
            traceResult.setAdtCommitEnd(System.nanoTime());
    }

    // record the start time of each handler in the complex-query
    public void setHandlerStart(DMLResponseHandler handler) {
        if (isTrace)
            traceResult.addToRecordStartMap(handler, System.nanoTime());
    }

    // record the end time of each handler in the complex-query
    public void setHandlerEnd(DMLResponseHandler handler) {
        if (handler.getNextHandler() != null) {
            DMLResponseHandler next = handler.getNextHandler();
            sessionStage = SessionStage.changeFromHandlerType(next.type());
        }
        if (isTrace)
            traceResult.addToRecordEndMap(handler, System.nanoTime());
    }

    public void setTraceBuilder(BaseHandlerBuilder baseBuilder) {
        if (isTrace)
            traceResult.setBuilder(baseBuilder);
    }

    public void setTraceSimpleHandler(ResponseHandler simpleHandler) {
        if (isTrace)
            traceResult.setSimpleHandler(simpleHandler);
    }

    /*private void sqlTracking(Consumer<TraceResult> consumer) {
        try {
            if (isTrace) {
                if (traceResult != null) {
                    consumer.accept(traceResult);
                }
            }
        } catch (Exception e) {
            // Should not affect the main task
            LOGGER.warn("sqlTracking occurred ", e);
        }
    }*/

    /*private void sqlCosting(Consumer<QueryTimeCost> costConsumer) {
        try {
            if (timeCost) {
                if (queryTimeCost != null) {
                    costConsumer.accept(queryTimeCost);
                }
            }
        } catch (Exception e) {
            // Should not affect the main task
            LOGGER.warn("sqlCosting occurred ", e);
        }
    }*/

    public SessionStage getSessionStage() {
        return sessionStage;
    }
}
