/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TraceResult implements Cloneable {
    public enum SqlTraceType {
        SINGLE_NODE_QUERY, MULTI_NODE_QUERY, MULTI_NODE_GROUP, COMPLEX_QUERY, SIMPLE_QUERY, COMPLEX_MODIFY
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceResult.class);
    protected long requestStart;
    protected long requestStartMs;
    protected long parseStart; // requestEnd
    protected long routeStart; // parseEnd
    protected long preExecuteStart; //r outeEnd
    protected RouteResultsetNode[] shardingNodes;
    protected long preExecuteEnd;

    protected long adtCommitBegin; // auto Distributed Transaction commit begin
    protected long adtCommitEnd; // auto Distributed Transaction commit end

    protected ResponseHandler simpleHandler = null;
    protected List<BackendRoute> backendRouteList = Lists.newCopyOnWriteArrayList();
    protected BaseHandlerBuilder builder = null; // for complex query
    protected Map<DMLResponseHandler, ComplexHandler> complexHandlerMap = Maps.newConcurrentMap();
    protected SqlTraceType type;
    protected long requestEnd;
    protected long requestEndMs;
    protected boolean subQuery = false;

    protected String sql;
    protected int sqlType = -1;
    protected String schema;
    protected ArrayList<String> tableList = new ArrayList<>();
    protected long sqlRows = 0;
    protected long netOutBytes;
    protected long resultSize;
    protected TraceResult previous = null;
    protected NonBlockingSession currentSession;
    /*
     * when 'set trace = 1' or 'enableSlowLog==true', need to record the time spent in each phase;
     *
     * so, into method:
     * endParse/endRoute/setPreExecuteEnd/setSubQuery/
     * setShardingNodes/setSimpleHandler/setBuilder/
     * setAdtCommitBegin/setAdtCommitEnd/addToRecordStartMap/addToRecordEndMap
     *
     */
    protected boolean isDetailTrace = false;
    // only samplingRate=100
    protected boolean pureRecordSql = true;

    public TraceResult(NonBlockingSession session0) {
        this.currentSession = session0;
    }

    public void setRequestTime(long time, long timeMs) {
        copyToPrevious();
        reset();
        this.isDetailTrace = currentSession.isTraceEnable() || SlowQueryLog.getInstance().isEnableSlowLog();
        this.pureRecordSql = !isDetailTrace && StatisticManager.getInstance().isPureRecordSql();
        this.requestStart = time;
        this.requestStartMs = timeMs;
    }

    public void startProcess(long time) {
        if (currentSession.getIsMultiStatement().get()) // multi-Query, need reset
            reset();
        this.parseStart = time;
    }

    public void setQuery(String sql0, int sqlType0) {
        this.schema = currentSession.getShardingService().getSchema();
        this.sql = sql0;
        this.sqlType = sqlType0;
    }

    public void addTable(List<Pair<String, String>> tables) { // schema.table
        for (Pair<String, String> p : tables) {
            tableList.add(p.getKey() + "." + p.getValue());
        }
    }

    public void endParse(long time) {
        if (!isDetailTrace) return;
        this.routeStart = time;
    }

    public void endRoute(long time) {
        if (!isDetailTrace) return;
        this.preExecuteStart = time;
    }

    public void setPreExecuteEnd(SqlTraceType type0, long time) {
        if (!isDetailTrace) return;
        if (routeStart != 0 && preExecuteStart != 0)
            this.type = type0;
        this.preExecuteEnd = time;
        backendRouteList.clear();
    }

    public void setSubQuery() {
        if (!isDetailTrace) return;
        this.subQuery = true;
    }

    public void setBackendRequestTime(MySQLResponseService service, long time) {
        final ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getTraceRouteKey();
            BackendRoute ar = null;
            for (BackendRoute b : backendRouteList) {
                if (b.handler == responseHandler && b.routeKey.equals(key)) {
                    ar = b;
                    break;
                }
            }
            if (ar == null) {
                ar = new BackendRoute(responseHandler, key, node.getName(), node.getStatement(), time);
                backendRouteList.add(ar);
            }
        }
    }

    public void setBackendResponseTime(MySQLResponseService service, long time) {
        final ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            String key = service.getTraceRouteKey();
            BackendRoute ar = null;
            for (BackendRoute b : backendRouteList) {
                if (b.handler == responseHandler && b.routeKey.equals(key) && b.firstRevTime == 0) {
                    ar = b;
                    break;
                }
            }
            if (ar != null) {
                ar.setFirstRevTime(time);
                ar.setMysqlResponseService(service);
            }
        }
    }

    public void setBackendSqlAddRows(MySQLResponseService service, Long num) {
        final ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            String key = service.getTraceRouteKey();
            BackendRoute ar = null;
            for (BackendRoute b : backendRouteList) {
                if (b.handler == responseHandler && b.routeKey.equals(key) && b.firstRevTime != 0) {
                    ar = b;
                    break;
                }
            }
            if (ar != null) {
                if (num == null) {
                    ar.getRow().incrementAndGet();
                } else {
                    ar.getRow().set(num);
                }
            }
        }
    }

    public void setBackendResponseEndTime(MySQLResponseService service, long time) {
        ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getTraceRouteKey();
            BackendRoute ar = null;
            for (BackendRoute b : backendRouteList) {
                if (b.handler == responseHandler && b.routeKey.equals(key) && b.firstRevTime != 0 && b.finished == 0) {
                    ar = b;
                    break;
                }
            }
            if (ar != null) {
                ar.setFinished(time);
                ar.setAutocommit(service.isAutocommit());
                if (pureRecordSql) return;
                StatisticBackendSqlEntry bEntry = new StatisticBackendSqlEntry(
                        currentSession.getTraceFrontendInfo(),
                        service.getConnection().getTraceBackendInfo(), node.getName(),
                        ar.getRequestTime(), ar.getSql(), node.getSqlType(), ar.getRow().get(), ar.getFinished());
                bEntry.setNeedToTx(ar.isAutocommit());
                StatisticManager.getInstance().push(bEntry);
            }
        }
    }

    public void setBackendTerminateByComplex(MultiNodeMergeHandler mergeHandler, long time) {
        for (BaseDMLHandler handler : mergeHandler.getExeHandlers()) {
            BackendRoute ar = null;
            for (BackendRoute b : backendRouteList) {
                if (b.handler == handler && b.firstRevTime != 0 && b.finished == 0) {
                    ar = b;
                    break;
                }
            }
            if (ar != null) {
                ar.setFinished(time);
                if (pureRecordSql) return;
                MySQLResponseService service;
                if ((service = ar.getMysqlResponseService()) != null) {
                    RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
                    if (node != null) {
                        ar.setAutocommit(service.isAutocommit());
                        StatisticBackendSqlEntry bEntry = new StatisticBackendSqlEntry(
                                currentSession.getTraceFrontendInfo(),
                                service.getConnection().getTraceBackendInfo(), node.getName(),
                                ar.getRequestTime(), ar.getSql(), node.getSqlType(), ar.getRow().get(), ar.getFinished());
                        bEntry.setNeedToTx(ar.isAutocommit());
                        StatisticManager.getInstance().push(bEntry);
                    }
                }
            }
        }
    }

    // commit、rollback、quit
    public void setBackendResponseTxEnd(MySQLResponseService service, long time) {
        if (pureRecordSql) return;
        if (!service.isAutocommit()) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            StatisticBackendSqlEntry bEntry = new StatisticBackendSqlEntry(
                    currentSession.getTraceFrontendInfo(),
                    service.getConnection().getTraceBackendInfo(), node.getName(),
                    time, "/** txEnd **/", 0, 0, time);
            bEntry.setNeedToTx(true);
            StatisticManager.getInstance().push(bEntry);
        }
    }

    public void setFrontendAddRows() {
        this.sqlRows += 1;
    }

    public void setFrontendSetRows(long rows) {
        this.sqlRows = rows;
    }

    public void setSqlStat(long sqlRows0, long netOutBytes0, long resultSize0) {
        //this.sqlRows = sqlRows0;
        this.netOutBytes = netOutBytes0;
        this.resultSize = resultSize0;
    }

    public void setResponseTime(boolean isSuccess, long time, long timeMs) {
        if (this.requestEnd == 0) {
            this.requestEnd = time;
            this.requestEndMs = timeMs;
            if (this.isCompletedV1() && isSuccess) {
                long examinedRows = getExaminedRows();
                StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(currentSession.getTraceFrontendInfo(), requestStart, requestStartMs,
                        schema, sql, sqlType, currentSession.getShardingService().getTxId(), examinedRows, sqlRows,
                        netOutBytes, resultSize, requestEnd, requestEndMs, new ArrayList<String>(tableList));
                StatisticManager.getInstance().push(f);
                if (isDetailTrace) {
                    SlowQueryLog.getInstance().putSlowQueryLog(currentSession.getShardingService(), this.clone());
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("try to record sql: {}", sql);
                }
            }
        }
    }

    private long getExaminedRows() {
        long examinedRows = 0;
        for (BackendRoute backendRoute : backendRouteList) {
            if (backendRoute.finished != 0) {
                examinedRows += backendRoute.getRow().get();
            }
        }
        return examinedRows;
    }

    public void setExit() {
        reset();
        previous = null;
    }

    public void setShardingNodes(RouteResultsetNode[] shardingNodes) {
        if (!isDetailTrace) return;
        if (this.shardingNodes == null) {
            this.shardingNodes = shardingNodes;
        } else {
            RouteResultsetNode[] tempShardingNodes = new RouteResultsetNode[this.shardingNodes.length + shardingNodes.length];
            System.arraycopy(this.shardingNodes, 0, tempShardingNodes, 0, this.shardingNodes.length);
            System.arraycopy(shardingNodes, 0, tempShardingNodes, this.shardingNodes.length, shardingNodes.length);
            this.shardingNodes = tempShardingNodes;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("append to existing shardingNodes,current size is " + this.shardingNodes.length);
            }
        }
    }

    public void setSimpleHandler(ResponseHandler simpleHandler) {
        if (!isDetailTrace) return;
        this.simpleHandler = simpleHandler;
    }

    public void setBuilder(BaseHandlerBuilder builder) {
        if (!isDetailTrace) return;
        this.builder = builder;
    }

    public void setAdtCommitBegin(long time) {
        if (!isDetailTrace) return;
        this.adtCommitBegin = time;
    }

    public void setAdtCommitEnd(long time) {
        if (!isDetailTrace) return;
        this.adtCommitEnd = time;
    }

    public void addToRecordStartMap(DMLResponseHandler handler, long time) {
        if (!isDetailTrace) return;
        complexHandlerMap.putIfAbsent(handler, new ComplexHandler(handler, time));
    }

    public void addToRecordEndMap(DMLResponseHandler handler, long time) {
        if (!isDetailTrace) return;
        if (complexHandlerMap.containsKey(handler)) {
            complexHandlerMap.get(handler).setEndTime(time);
        }
    }

    private void reset() {
        parseStart = 0;
        routeStart = 0;
        preExecuteStart = 0;
        preExecuteEnd = 0;
        requestEnd = 0;
        requestEndMs = 0;
        shardingNodes = null;
        adtCommitBegin = 0;
        adtCommitEnd = 0;
        sql = null;
        sqlType = -1;
        schema = null;
        tableList.clear();
        sqlRows = 0;
        netOutBytes = 0;
        resultSize = 0;
        type = null;
        subQuery = false;
        simpleHandler = null;
        builder = null;
        backendRouteList.clear();
        complexHandlerMap.clear();
        isDetailTrace = false;
    }

    private void copyToPrevious() {
        if (isDetailTrace) {
            this.previous = this.clone();
            if (!previous.isCompletedV1()) {
                previous = null;
            }
        } else {
            previous = null;
        }
    }

    public List<String[]> genRunningSQLStage() {
        return (new SelectTraceResult(this)).genRunningSQLStage();
    }

    // show trace
    public List<String[]> genShowTraceResult() {
        try {
            if (this.previous != null) {
                return (new SelectTraceResult(this.previous)).genTraceResult();
            }
            return null;
        } catch (Exception e) {
            LOGGER.warn("genShowTraceResult exception {}", e);
            return null;
        } finally {
            this.previous = null;
        }
    }

    // slow log
    public List<String[]> genLogResult() {
        return (new SelectTraceResult(this)).genLogResult();
    }

    protected boolean isCompletedV1() {
        return sql != null && this.requestStart != 0 && this.requestEnd != 0;
    }

    protected boolean isCompletedV2() {
        if (isCompletedV1() && this.routeStart != 0) {
            long firstRevCount = this.backendRouteList.stream().filter(f -> f.getFirstRevTime() != 0).count();
            if (firstRevCount != 0) {
                long finishedCount = this.backendRouteList.stream().filter(f -> f.getFinished() != 0).count();
                if (firstRevCount == finishedCount) {
                    long recordStartCount = this.complexHandlerMap.size();
                    long recordEndCount = this.complexHandlerMap.values().stream().filter(f -> f.endTime != 0).count();
                    return recordStartCount == recordEndCount;
                }
            }
        }
        return false;
    }

    protected boolean isNonBusinessSql() {
        return type == null; // || routeStart == null;
    }

    public RouteResultsetNode[] getShardingNodes() {
        return shardingNodes;
    }

    public SqlTraceType getType() {
        if (this.type == null) {
            return SqlTraceType.SIMPLE_QUERY;
        }
        return this.type;
    }

    public String getOverAllSecond() {
        double milliSecond = (double) (this.requestEnd - this.requestStart) / 1000000000;
        return String.format("%.6f", milliSecond);
    }

    @Override
    public TraceResult clone() {
        TraceResult tr;
        try {
            tr = (TraceResult) super.clone();
            tr.previous = null;
            tr.simpleHandler = this.simpleHandler;
            tr.builder = this.builder;
            tr.backendRouteList = new CopyOnWriteArrayList(this.backendRouteList);
            tr.complexHandlerMap = new ConcurrentHashMap<>(this.complexHandlerMap);
            tr.isDetailTrace = this.isDetailTrace;
            return tr;
        } catch (Exception e) {
            LOGGER.warn("clone TraceResult error", e);
            throw new AssertionError(e.getMessage());
        }
    }

    // find handler
    protected List<BackendRoute> findByHandler(ResponseHandler handler0) {
        return this.backendRouteList.stream().filter(f -> f.handler == handler0).collect(Collectors.toList());
    }

    protected ComplexHandler findByComplexHandler(ResponseHandler handler0) {
        return complexHandlerMap.get(handler0);
    }

    protected static class BackendRoute {
        ResponseHandler handler;
        MySQLResponseService mysqlResponseService;
        String routeKey;
        String shardingNode;
        String sql;
        long requestTime;
        long firstRevTime;
        long finished;
        AtomicLong row;
        boolean autocommit;

        public BackendRoute(ResponseHandler handler, String routeKey, String shardingNode, String sql, long requestTime) {
            this.handler = handler;
            this.routeKey = routeKey;
            this.requestTime = requestTime;
            this.shardingNode = shardingNode;
            this.sql = sql;
            this.row = new AtomicLong(0);
        }

        public long getRequestTime() {
            return requestTime;
        }

        public long getFirstRevTime() {
            return firstRevTime;
        }

        public void setFirstRevTime(long firstRevTime) {
            this.firstRevTime = firstRevTime;
        }

        public long getFinished() {
            return finished;
        }

        public void setFinished(long finished) {
            this.finished = finished;
        }

        public AtomicLong getRow() {
            return row;
        }

        public String getShardingNode() {
            return shardingNode;
        }

        public String getSql() {
            return sql;
        }

        public boolean isAutocommit() {
            return autocommit;
        }

        public void setAutocommit(boolean autocommit) {
            this.autocommit = autocommit;
        }

        public MySQLResponseService getMysqlResponseService() {
            return mysqlResponseService;
        }

        public void setMysqlResponseService(MySQLResponseService mysqlResponseService) {
            this.mysqlResponseService = mysqlResponseService;
        }
    }

    protected static class ComplexHandler {
        DMLResponseHandler handler;
        long startTime;
        long endTime;

        public ComplexHandler(DMLResponseHandler handler, long startTime) {
            this.handler = handler;
            this.startTime = startTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public DMLResponseHandler getHandler() {
            return handler;
        }
    }
}
