/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TraceResult {
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
    private ConcurrentMap<MySQLConnection, Boolean> connFlagMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> connReceivedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> connFinishedMap = new ConcurrentHashMap<>();
    private Map<ResponseHandler, TraceRecord> recordStartMap = new ConcurrentHashMap<>();
    private Map<ResponseHandler, TraceRecord> recordEndMap = new ConcurrentHashMap<>();
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

    public void setAdtCommitBegin(TraceRecord adtCommitBegin) {
        this.adtCommitBegin = adtCommitBegin;
    }

    public void setAdtCommitEnd(TraceRecord adtCommitEnd) {
        this.adtCommitEnd = adtCommitEnd;
    }
    public ConcurrentMap<MySQLConnection, Boolean> getConnFlagMap() {
        return connFlagMap;
    }


    public ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> getConnReceivedMap() {
        return connReceivedMap;
    }

    public ConcurrentMap<ResponseHandler, Map<MySQLConnection, TraceRecord>> getConnFinishedMap() {
        return connFinishedMap;
    }


    public Map<ResponseHandler, TraceRecord> getRecordStartMap() {
        return recordStartMap;
    }

    public Map<ResponseHandler, TraceRecord> getRecordEndMap() {
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
        if (veryEnd == 0 || connFlagMap.size() == 0 || connReceivedMap.size() != connFinishedMap.size() || recordStartMap.size() != recordEndMap.size()) {
            return null;
        }
        List<String[]> lst = new ArrayList<>();
        lst.add(genTraceRecord("Read SQL", requestStart.getTimestamp(), parseStart.getTimestamp()));
        lst.add(genTraceRecord("Parse SQL", parseStart.getTimestamp(), routeStart.getTimestamp()));
        lst.add(genTraceRecord("Route Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Try to Optimize", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        if (simpleHandler != null) {
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
        lst.add(genTraceRecord("Over All", veryStart, veryEnd));
        clear();
        return lst;
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
        readQuery[5] = ref;
        return readQuery;
    }

    private String nanoToMilliSecond(long nano) {
        double milliSecond = (double) nano / 1000000;
        return String.valueOf(milliSecond);
    }
}
