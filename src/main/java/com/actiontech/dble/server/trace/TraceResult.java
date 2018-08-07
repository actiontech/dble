/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.BaseSelectHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.plan.util.ComplexQueryPlanUtil;
import com.actiontech.dble.plan.util.ReferenceHandlerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TraceResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceResult.class);
    private long veryStartPrepare;
    private long veryStart;
    private TraceRecord requestStartPrepare;
    private TraceRecord requestStart;
    private TraceRecord parseStartPrepare; //requestEnd
    private TraceRecord parseStart; //requestEnd
    private TraceRecord routeStart; //parseEnd
    private TraceRecord preExecuteStart; //routeEnd
    private TraceRecord preExecuteEnd;

    private TraceRecord adtCommitBegin; //auto Distributed Transaction commit begin
    private TraceRecord adtCommitEnd; ////auto Distributed Transaction commit end

    private ResponseHandler simpleHandler = null;
    private BaseHandlerBuilder builder = null; //for complex query
    private ConcurrentMap<String, Boolean> connFlagMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> connReceivedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> connFinishedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<DMLResponseHandler, TraceRecord> recordStartMap = new ConcurrentHashMap<>();
    private ConcurrentMap<DMLResponseHandler, TraceRecord> recordEndMap = new ConcurrentHashMap<>();
    private long veryEnd;

    public void setVeryStartPrepare(long veryStartPrepare) {
        this.veryStartPrepare = veryStartPrepare;
    }

    public void setRequestStartPrepare(TraceRecord requestStartPrepare) {
        this.requestStartPrepare = requestStartPrepare;
    }

    public void setRouteStart(TraceRecord routeStart) {
        this.routeStart = routeStart;
    }

    public void setParseStartPrepare(TraceRecord parseStartPrepare) {
        this.parseStartPrepare = parseStartPrepare;
    }

    public void setPreExecuteStart(TraceRecord preExecuteStart) {
        this.preExecuteStart = preExecuteStart;
    }

    public void setPreExecuteEnd(TraceRecord preExecuteEnd) {
        this.preExecuteEnd = preExecuteEnd;
    }

    public void setSimpleHandler(ResponseHandler simpleHandler) {
        this.simpleHandler = simpleHandler;
    }

    public void setBuilder(BaseHandlerBuilder builder) {
        this.builder = builder;
    }

    public void setAdtCommitBegin(TraceRecord adtCommitBegin) {
        this.adtCommitBegin = adtCommitBegin;
    }

    public void setAdtCommitEnd(TraceRecord adtCommitEnd) {
        this.adtCommitEnd = adtCommitEnd;
    }
    public ConcurrentMap<String, Boolean> getConnFlagMap() {
        return connFlagMap;
    }


    public ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> getConnReceivedMap() {
        return connReceivedMap;
    }

    public ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> getConnFinishedMap() {
        return connFinishedMap;
    }


    public ConcurrentMap<DMLResponseHandler, TraceRecord> getRecordStartMap() {
        return recordStartMap;
    }

    public ConcurrentMap<DMLResponseHandler, TraceRecord> getRecordEndMap() {
        return recordEndMap;
    }

    public void setVeryEnd(long veryEnd) {
        this.veryEnd = veryEnd;
    }

    public void ready() {
        clear();
        veryStart = veryStartPrepare;
        requestStart = requestStartPrepare;
        parseStart = parseStartPrepare;
    }

    private void clear() {
        veryStart = 0;
        requestStart = null;
        parseStart = null;
        routeStart = null;
        preExecuteStart = null;
        preExecuteEnd = null;
        adtCommitBegin = null;
        adtCommitEnd = null;

        simpleHandler = null;
        builder = null; //for complex query
        connFlagMap.clear();
        for (Map<MySQLConnection, TraceRecord> connReceived : connReceivedMap.values()) {
            connReceived.clear();
        }
        connReceivedMap.clear();
        for (Map<MySQLConnection, TraceRecord> connReceived : connFinishedMap.values()) {
            connReceived.clear();
        }
        connFinishedMap.clear();
        recordStartMap.clear();
        recordEndMap.clear();
        veryEnd = 0;
    }

    public List<String[]> genTraceResult() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start genTraceResult");
        }
        if (veryEnd == 0 || connFlagMap.size() == 0 || connReceivedMap.size() != connFinishedMap.size() || recordStartMap.size() != recordEndMap.size()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("collect info not in pairs,veryEnd:" + veryEnd + ",connFlagMap.size:" + connFlagMap.size() +
                        ",connReceivedMap.size:" + connReceivedMap.size() + ",connFinishedMap.size:" + connFinishedMap.size() +
                        ",recordStartMap.size:" + connReceivedMap.size() + ",recordEndMap.size:" + connFinishedMap.size());
            }
            return null;
        }
        List<String[]> lst = new ArrayList<>();
        lst.add(genTraceRecord("Read SQL", requestStart.getTimestamp(), parseStart.getTimestamp()));
        lst.add(genTraceRecord("Parse SQL", parseStart.getTimestamp(), routeStart.getTimestamp()));
        if (simpleHandler != null) {
            genSimpleResults(lst);
        } else if (builder != null) {
            if (genComplexQueryResults(lst)) return null;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not support trace this query");
            }
            return null;
        }
        lst.add(genTraceRecord("Over All", veryStart, veryEnd));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end genTraceResult");
        }
        clear();
        return lst;
    }

    private boolean genComplexQueryResults(List<String[]> lst) {
        lst.add(genTraceRecord("Try Route Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Try to Optimize", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<MySQLConnection, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<MySQLConnection, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
                if (fetchStartRecordMap == null || fetchEndRecordMap == null || fetchStartRecordMap.size() != 1 || fetchEndRecordMap.size() != 1) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for connection");
                    }
                    return true;
                }
                TraceRecord fetchStartRecord = fetchStartRecordMap.values().iterator().next();
                TraceRecord fetchEndRecord = fetchEndRecordMap.values().iterator().next();
                if (!result.isNestLoopQuery()) {
                    lst.add(genTraceRecord("Execute SQL", lastChildFinished, fetchStartRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
                } else {
                    TraceRecord handlerStart = recordStartMap.get(handler);
                    TraceRecord handlerEnd = recordEndMap.get(handler);
                    if (handlerStart == null || handlerEnd == null) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("collect info not in pairs for handler" + handler);
                        }
                        return true;
                    }
                    lst.add(genTraceRecord("Generate New Query", lastChildFinished, handlerStart.getTimestamp()));
                    lst.add(genTraceRecord("Execute SQL", handlerStart.getTimestamp(), handlerEnd.getTimestamp(), result.getName(), result.getRefOrSQL()));
                }
                lst.add(genTraceRecord("Fetch result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
            } else if (handler instanceof OutputHandler) {
                TraceRecord startWrite = recordStartMap.get(handler);
                if (startWrite == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for OutputHandler");
                    }
                    return true;
                }
                lst.add(genTraceRecord("Write to Client", startWrite.getTimestamp(), veryEnd));
            } else {
                TraceRecord handlerStart = recordStartMap.get(handler);
                TraceRecord handlerEnd = recordEndMap.get(handler);
                if (handlerStart == null || handlerEnd == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for handler" + handler);
                    }
                    return true;
                }
                lst.add(genTraceRecord(result.getType(), handlerStart.getTimestamp(), handlerEnd.getTimestamp(), result.getName(), result.getRefOrSQL()));
                if (handler.getNextHandler() == null) {
                    lastChildFinished = Math.max(lastChildFinished, handlerEnd.getTimestamp());
                }
            }
        }
        return false;
    }

    private void genSimpleResults(List<String[]> lst) {
        lst.add(genTraceRecord("Route Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Prepare to Push", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        Map<MySQLConnection, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<MySQLConnection, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<MySQLConnection, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
            TraceRecord fetchStartRecord = fetchStart.getValue();
            minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
            executeList.add(genTraceRecord("Execute SQL", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp(), fetchStartRecord.getDataNode(), fetchStartRecord.getRef()));
            TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
            fetchList.add(genTraceRecord("Fetch result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), fetchStartRecord.getDataNode(), fetchStartRecord.getRef()));
            maxFetchEnd = Math.max(maxFetchEnd, fetchEndRecord.getTimestamp());
        }
        lst.addAll(executeList);
        lst.addAll(fetchList);
        if (adtCommitBegin != null) {
            lst.add(genTraceRecord("Distributed Transaction Prepare", maxFetchEnd, adtCommitBegin.getTimestamp()));
            lst.add(genTraceRecord("Distributed Transaction Commit", adtCommitBegin.getTimestamp(), adtCommitEnd.getTimestamp()));
        }
        lst.add(genTraceRecord("Write to Client", minFetchStart, veryEnd));
    }

    private String[] genTraceRecord(String operation, long start, long end) {
        return genTraceRecord(operation, start, end, "-", "-");
    }

    private String[] genTraceRecord(String operation, long start, long end, String dataNode, String ref) {
        String[] readQuery = new String[6];
        readQuery[0] = operation;
        readQuery[1] = nanoToMilliSecond(start - veryStart);
        readQuery[2] = nanoToMilliSecond(end - veryStart);
        readQuery[3] = nanoToMilliSecond(end - start);
        readQuery[4] = dataNode;
        readQuery[5] = ref.replaceAll("[\\t\\n\\r]", " ");
        return readQuery;
    }

    private String nanoToMilliSecond(long nano) {
        double milliSecond = (double) nano / 1000000;
        return String.valueOf(milliSecond);
    }
}
