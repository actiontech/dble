/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RwTraceResult implements Cloneable, ITraceResult {


    private static final Logger LOGGER = LoggerFactory.getLogger(RwTraceResult.class);
    //    private boolean prepareFinished = false;
    private long veryStartPrepare;
    private long veryStart;
    private TraceRecord requestStartPrepare;
    private TraceRecord requestStart;
    private TraceRecord parseStartPrepare; //requestEnd
    private TraceRecord parseStart; //requestEnd
    //    private TraceRecord routeStart; //parseEnd
    //    private TraceRecord preExecuteStart; //routeEnd
    private TraceRecord preExecuteEnd;


    private ResponseHandler simpleHandler = null;
    private ConcurrentMap<String, Boolean> connFlagMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<String, TraceRecord>> connReceivedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<String, TraceRecord>> connFinishedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, TraceRecord> recordStartMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, TraceRecord> recordEndMap = new ConcurrentHashMap<>();
    private long veryEnd;
    private SqlTraceType type = SqlTraceType.RWSPLIT_QUERY;
    private PhysicalDbInstance dbInstance = null;

    public void setVeryStartPrepare(long veryStartPrepare) {
        //        prepareFinished = false;
        this.veryStartPrepare = veryStartPrepare;
        this.requestStartPrepare = new TraceRecord(veryStartPrepare);
    }

    public void setDBInstance(PhysicalDbInstance dbInstance) {
        this.dbInstance = dbInstance;
    }

    public void setParseStartPrepare(TraceRecord parseStartPrepare) {
        this.parseStartPrepare = parseStartPrepare;
    }


    public void setPreExecuteEnd(TraceRecord preExecuteEnd) {
        this.preExecuteEnd = preExecuteEnd;
    }


    public void setSimpleHandler(ResponseHandler simpleHandler) {
        this.simpleHandler = simpleHandler;
    }


    public void setType(SqlTraceType type) {
        this.type = type;
    }


    public Boolean addToConnFlagMap(String item) {
        return connFlagMap.putIfAbsent(item, true);
    }

    public void clearConnFlagMap() {
        connFlagMap.clear();
    }

    public void addToConnReceivedMap(ResponseHandler responseHandler, Map<String, TraceRecord> connMap) {
        Map<String, TraceRecord> existReceivedMap = connReceivedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public void clearConnReceivedMap() {
        connReceivedMap.clear();
    }

    public void addToConnFinishedMap(ResponseHandler responseHandler, Map<String, TraceRecord> connMap) {
        Map<String, TraceRecord> existReceivedMap = connFinishedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public void addToRecordStartMap(ResponseHandler handler, TraceRecord traceRecord) {
        recordStartMap.putIfAbsent(handler, traceRecord);
    }

    public void addToRecordEndMap(ResponseHandler handler, TraceRecord traceRecord) {
        recordEndMap.putIfAbsent(handler, traceRecord);
    }

    public void setVeryEnd(long veryEnd) {
        this.veryEnd = veryEnd;
    }

    public void ready() {
        //        prepareFinished = true;
        clear();
        veryStart = veryStartPrepare;
        requestStart = requestStartPrepare;
        parseStart = parseStartPrepare;
        veryStartPrepare = 0;
        requestStartPrepare = null;
        parseStartPrepare = null;
    }

    private void clear() {
        veryStart = 0;
        requestStart = null;
        parseStart = null;
        preExecuteEnd = null;
        this.type = null;
        simpleHandler = null;
        connFlagMap.clear();
        for (Map<String, TraceRecord> connReceived : connReceivedMap.values()) {
            connReceived.clear();
        }
        connReceivedMap.clear();
        for (Map<String, TraceRecord> connReceived : connFinishedMap.values()) {
            connReceived.clear();
        }
        connFinishedMap.clear();
        recordStartMap.clear();
        recordEndMap.clear();
        veryEnd = 0;
    }


    @Override
    public boolean isCompleted() {
        return veryStart != 0 && veryEnd != 0 && connFlagMap.size() != 0 && connReceivedMap.size() == connFinishedMap.size() && recordStartMap.size() == recordEndMap.size();
    }

    @Override
    public SqlTraceType getType() {
        return this.type;
    }

    @Override
    public List<String[]> genLogResult() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start genLogResult");
        }
        if (!isCompleted()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("collect info not in pairs,veryEnd:" + veryEnd + ",connFlagMap.size:" + connFlagMap.size() +
                        ",connReceivedMap.size:" + connReceivedMap.size() + ",connFinishedMap.size:" + connFinishedMap.size() +
                        ",recordStartMap.size:" + connReceivedMap.size() + ",recordEndMap.size:" + connFinishedMap.size());
            }
            return null;
        }
        List<String[]> lst = new ArrayList<>();
        lst.add(genLogRecord("Read_SQL", requestStart.getTimestamp(), parseStart.getTimestamp()));
        lst.add(genLogRecord("Prepare_Push", parseStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        if (simpleHandler != null) {
            if (genSimpleLogs(lst)) return null;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not support trace this query");
            }
            return null;
        }

        lst.add(genLogRecord("Group_Name", dbInstance.getDbGroup().getGroupName()));
        lst.add(genLogRecord("Instance_Name", dbInstance.getName()));
        lst.add(genLogRecord("Is_Master", String.valueOf(!dbInstance.isReadInstance())));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end genLogResult");
        }
        return lst;
    }


    private boolean genSimpleLogs(List<String[]> lst) {
        Map<String, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<String, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<String, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
            TraceRecord fetchStartRecord = fetchStart.getValue();
            minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
            executeList.add(genLogRecord("First_Result_Fetch", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp()));
            TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
            if (fetchEndRecord == null) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genLogRecord("Last_Result_Fetch", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp()));
            maxFetchEnd = Math.max(maxFetchEnd, fetchEndRecord.getTimestamp());
        }
        lst.addAll(executeList);
        lst.addAll(fetchList);
        lst.add(genLogRecord("Write_Client", minFetchStart, veryEnd));
        return false;
    }

    private String[] genLogRecord(String operation, long start, long end) {
        String[] readQuery = new String[2];
        readQuery[0] = operation;
        readQuery[1] = nanoToSecond(end - start);
        return readQuery;
    }

    private String[] genLogRecord(String operation, String value) {
        String[] readQuery = new String[2];
        readQuery[0] = operation;
        readQuery[1] = value;
        return readQuery;
    }

    @Override
    public double getOverAllMilliSecond() {
        return (double) (veryEnd - veryStart) / 1000000;
    }

    private String nanoToSecond(long nano) {
        double milliSecond = (double) nano / 1000000000;
        return String.format("%.6f", milliSecond);
    }

    @Override
    public String getOverAllSecond() {
        return nanoToSecond(veryEnd - veryStart);
    }

    @Override
    public Object clone() {
        RwTraceResult tr;
        try {
            tr = (RwTraceResult) super.clone();
            tr.simpleHandler = this.simpleHandler;
            tr.connFlagMap = new ConcurrentHashMap<>();
            tr.connFlagMap.putAll(this.connFlagMap);
            tr.connReceivedMap = new ConcurrentHashMap<>();
            for (Map.Entry<ResponseHandler, Map<String, TraceRecord>> item : connReceivedMap.entrySet()) {
                Map<String, TraceRecord> connMap = new ConcurrentHashMap<>();
                connMap.putAll(item.getValue());
                tr.connReceivedMap.put(item.getKey(), connMap);
            }
            tr.connFinishedMap = new ConcurrentHashMap<>();
            for (Map.Entry<ResponseHandler, Map<String, TraceRecord>> item : connFinishedMap.entrySet()) {
                Map<String, TraceRecord> connMap = new ConcurrentHashMap<>();
                connMap.putAll(item.getValue());
                tr.connFinishedMap.put(item.getKey(), connMap);
            }
            tr.recordStartMap = new ConcurrentHashMap<>();
            tr.recordStartMap.putAll(this.recordStartMap);
            tr.recordEndMap = new ConcurrentHashMap<>();
            tr.recordEndMap.putAll(this.recordEndMap);
            return tr;
        } catch (Exception e) {
            LOGGER.warn("clone TraceResult error", e);
            throw new AssertionError(e.getMessage());
        }
    }
}
