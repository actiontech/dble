package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.SessionStage;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.stat.QueryTimeCost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

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
        sqlTracking(t -> t.setRequestTime(requestTime, System.currentTimeMillis()));
        timeCost = (SystemConfig.getInstance().getUseCostTimeStat() != 0) && !(ThreadLocalRandom.current().nextInt(100) >= SystemConfig.getInstance().getCostSamplePercent());
        sqlCosting(c -> c.setRequestTime(requestTime));
    }

    public void startProcess() {
        sessionStage = SessionStage.Parse_SQL;
        long startProcess = System.nanoTime();
        sqlTracking(t -> t.startProcess(startProcess));
        sqlCosting(c -> c.startProcess());
    }

    public void setQuery(String sql) {
        sqlTracking(t -> t.setQuery(sql));
    }

    public void endParse() {
        sessionStage = SessionStage.Route_Calculation;
        sqlTracking(t -> t.endParse(System.nanoTime()));
        sqlCosting(c -> c.endParse());
    }

    public void endRoute(RouteResultset rrs) {
        sessionStage = SessionStage.Prepare_to_Push;
        sqlTracking(t -> t.endRoute(System.nanoTime()));
        sqlCosting(c -> c.endRoute(rrs));
    }

    public void endComplexRoute() {
        sqlCosting(c -> c.endComplexRoute());
    }

    public void endComplexExecute() {
        sqlCosting(c -> c.endComplexExecute());
    }

    public void readyToDeliver() {
        sqlCosting(c -> c.readyToDeliver());
    }

    public void setPreExecuteEnd(TraceResult.SqlTraceType type) {
        sessionStage = SessionStage.Execute_SQL;
        sqlTracking(t -> t.setPreExecuteEnd(type, System.nanoTime()));
    }

    public void setSubQuery() {
        sqlTracking(t -> t.setSubQuery());
    }

    public void setBackendRequestTime(MySQLResponseService service) {
        long requestTime0 = System.nanoTime();
        sqlTracking(t -> t.setBackendRequestTime(service, requestTime0));
        sqlCosting(c -> c.setBackendRequestTime(service.getConnection().getId(), requestTime0));
    }

    // receives the response package (before being pushed into BackendService.taskQueue)
    public void setBackendResponseTime(MySQLResponseService service) {
        sessionStage = SessionStage.Fetching_Result;
        long responseTime = System.nanoTime();
        sqlTracking(t -> t.setBackendResponseTime(service, responseTime));
        sqlCosting(c -> c.setBackendResponseTime(service.getConnection().getId(), responseTime));
    }

    // start processing the response package (the first package taken out of the BackendService.taskQueue)
    public void startExecuteBackend() {
        sqlCosting(c -> c.startExecuteBackend());
    }

    // When multiple nodes are queried, all nodes return the point in time of the EOF package
    public void allBackendConnReceive() {
        sqlCosting(c -> c.allBackendConnReceive());
    }

    public void setBackendSqlAddRows(MySQLResponseService service) {
        sqlTracking(t -> t.setBackendSqlAddRows(service, null));
    }

    public void setBackendSqlSetRows(MySQLResponseService service, long rows) {
        sqlTracking(t -> t.setBackendSqlAddRows(service, rows));
    }

    // the final response package received,(include connection is accidentally closed or released)
    public void setBackendResponseEndTime(MySQLResponseService service) {
        sessionStage = SessionStage.First_Node_Fetched_Result;
        sqlTracking(t -> t.setBackendResponseEndTime(service, System.nanoTime()));
        sqlCosting(c -> c.setBackendResponseEndTime());
    }

    public void setBackendResponseTxEnd(MySQLResponseService service) {
        sqlTracking(t -> t.setBackendResponseTxEnd(service, System.nanoTime()));
    }

    public void setBackendResponseClose(MySQLResponseService service) {
        sqlTracking(t -> t.setBackendResponseTxEnd(service, System.nanoTime()));
    }

    public void setFrontendAddRows() {
        sqlTracking(t -> t.setFrontendAddRows());
    }

    public void setFrontendSetRows(long rows) {
        sqlTracking(t -> t.setFrontendSetRows(rows));
    }

    // get the rows、 netOutBytes、resultSize information in the last handler
    public void doSqlStat(long sqlRows, long netOutBytes, long resultSize) {
        sqlTracking(t -> t.setSqlStat(sqlRows, netOutBytes, resultSize));
    }

    public void setResponseTime(boolean isSuccess) {
        sessionStage = SessionStage.Finished;
        long responseTime = System.nanoTime();
        sqlTracking(t -> t.setResponseTime(isSuccess, responseTime, System.currentTimeMillis()));
        sqlCosting(t -> t.setResponseTime(responseTime));
    }

    public void setExit() {
        sqlTracking(t -> t.setExit(System.nanoTime()));
    }

    public void setBeginCommitTime() {
        sessionStage = SessionStage.Distributed_Transaction_Commit;
        sqlTracking(t -> t.setAdtCommitBegin(System.nanoTime()));
    }

    public void setFinishedCommitTime() {
        sqlTracking(t -> t.setAdtCommitEnd(System.nanoTime()));
    }

    // record the start time of each handler in the complex-query
    public void setHandlerStart(DMLResponseHandler handler) {
        sqlTracking(t -> t.addToRecordStartMap(handler, System.nanoTime()));
    }

    // record the end time of each handler in the complex-query
    public void setHandlerEnd(DMLResponseHandler handler) {
        if (handler.getNextHandler() != null) {
            DMLResponseHandler next = handler.getNextHandler();
            sessionStage = SessionStage.changeFromHandlerType(next.type());
        }
        sqlTracking(t -> t.addToRecordEndMap(handler, System.nanoTime()));
    }

    public void setTraceBuilder(BaseHandlerBuilder baseBuilder) {
        sqlTracking(t -> t.setBuilder(baseBuilder));
    }

    public void setTraceSimpleHandler(ResponseHandler simpleHandler) {
        sqlTracking(t -> t.setSimpleHandler(simpleHandler));
    }

    private void sqlTracking(Consumer<TraceResult> consumer) {
        try {
            if (isTrace) {
                Optional.ofNullable(traceResult).ifPresent(consumer);
            }
        } catch (Exception e) {
            // Should not affect the main task
            LOGGER.warn("sqlTracking occurred ", e);
        }
    }

    private void sqlCosting(Consumer<QueryTimeCost> costConsumer) {
        try {
            if (timeCost) {
                Optional.ofNullable(queryTimeCost).ifPresent(costConsumer);
            }
        } catch (Exception e) {
            // Should not affect the main task
            LOGGER.warn("sqlCosting occurred ", e);
        }
    }

    public SessionStage getSessionStage() {
        return sessionStage;
    }
}
