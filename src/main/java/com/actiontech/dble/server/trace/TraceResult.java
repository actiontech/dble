/*
 * Copyright (C) 2016-2019 ActionTech.
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

public class TraceResult implements Cloneable {
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

    public synchronized Boolean addToConnFlagMap(String item) {
        return connFlagMap.putIfAbsent(item, true);
    }

    public synchronized void clearConnFlagMap() {
        connFlagMap.clear();
    }

    public synchronized void addToConnReceivedMap(ResponseHandler responseHandler, Map<MySQLConnection, TraceRecord> connMap) {
        Map<MySQLConnection, TraceRecord> existReceivedMap = connReceivedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public synchronized void clearConnReceivedMap() {
        connReceivedMap.clear();
    }

    public synchronized void addToConnFinishedMap(ResponseHandler responseHandler, Map<MySQLConnection, TraceRecord> connMap) {
        Map<MySQLConnection, TraceRecord> existReceivedMap = connFinishedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public synchronized void addToRecordStartMap(DMLResponseHandler handler, TraceRecord traceRecord) {
        recordStartMap.putIfAbsent(handler, traceRecord);
    }

    public synchronized void addToRecordEndMap(DMLResponseHandler handler, TraceRecord traceRecord) {
        recordEndMap.putIfAbsent(handler, traceRecord);
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
        if (!isCompleted()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("collect info not in pairs,veryEnd:" + veryEnd + ",connFlagMap.size:" + connFlagMap.size() +
                        ",connReceivedMap.size:" + connReceivedMap.size() + ",connFinishedMap.size:" + connFinishedMap.size() +
                        ",recordStartMap.size:" + recordStartMap.size() + ",recordEndMap.size:" + recordEndMap.size());
            }
            return null;
        }
        List<String[]> lst = new ArrayList<>();
        lst.add(genTraceRecord("Read_SQL", requestStart.getTimestamp(), parseStart.getTimestamp()));
        lst.add(genTraceRecord("Parse_SQL", parseStart.getTimestamp(), routeStart.getTimestamp()));
        if (simpleHandler != null) {
            if (genSimpleResults(lst)) return null;
        } else if (builder != null) {
            if (genComplexQueryResults(lst)) return null;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not support trace this query");
            }
            return null;
        }
        lst.add(genTraceRecord("Over_All", veryStart, veryEnd));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end genTraceResult");
        }
        clear();
        return lst;
    }

    private boolean genComplexQueryResults(List<String[]> lst) {
        lst.add(genTraceRecord("Try_Route_Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Try_to_Optimize", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<MySQLConnection, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<MySQLConnection, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
                if (fetchStartRecordMap == null || fetchEndRecordMap == null || fetchStartRecordMap.size() != 1 || fetchEndRecordMap.size() != 1) {
                    printNoResultDebug(fetchStartRecordMap, fetchEndRecordMap);
                    return true;
                }
                TraceRecord fetchStartRecord = fetchStartRecordMap.values().iterator().next();
                TraceRecord fetchEndRecord = fetchEndRecordMap.values().iterator().next();
                if (!result.isNestLoopQuery()) {
                    lst.add(genTraceRecord("Execute_SQL", lastChildFinished, fetchStartRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
                } else {
                    TraceRecord handlerStart = recordStartMap.get(handler);
                    TraceRecord handlerEnd = recordEndMap.get(handler);
                    if (handlerStart == null || handlerEnd == null) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("collect info not in pairs for handler" + handler);
                        }
                        return true;
                    }
                    lst.add(genTraceRecord("Generate_New_Query", lastChildFinished, handlerStart.getTimestamp()));
                    lst.add(genTraceRecord("Execute_SQL", handlerStart.getTimestamp(), handlerEnd.getTimestamp(), result.getName(), result.getRefOrSQL()));
                }
                lst.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
            } else if (handler instanceof OutputHandler) {
                TraceRecord startWrite = recordStartMap.get(handler);
                if (startWrite == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for OutputHandler");
                    }
                    return true;
                }
                lst.add(genTraceRecord("Write_to_Client", startWrite.getTimestamp(), veryEnd));
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

    private void printNoResultDebug(Map<MySQLConnection, TraceRecord> fetchStartRecordMap, Map<MySQLConnection, TraceRecord> fetchEndRecordMap) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("collect info not in pairs for connection");
            if (fetchStartRecordMap != null) {
                LOGGER.debug("fetchStartRecordMap size is " + fetchStartRecordMap.size());
            }
            if (fetchEndRecordMap != null) {
                LOGGER.debug("fetchEndRecordMap size is " + fetchEndRecordMap.size());
            }
        }
    }

    private boolean genSimpleResults(List<String[]> lst) {
        lst.add(genTraceRecord("Route_Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Prepare_to_Push", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        Map<MySQLConnection, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<MySQLConnection, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<MySQLConnection, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
            TraceRecord fetchStartRecord = fetchStart.getValue();
            minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
            executeList.add(genTraceRecord("Execute_SQL", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp(), fetchStartRecord.getDataNode(), fetchStartRecord.getRef()));
            TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
            if (fetchEndRecord == null) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), fetchStartRecord.getDataNode(), fetchStartRecord.getRef()));
            maxFetchEnd = Math.max(maxFetchEnd, fetchEndRecord.getTimestamp());
        }
        lst.addAll(executeList);
        lst.addAll(fetchList);
        if (adtCommitBegin != null) {
            lst.add(genTraceRecord("Distributed_Transaction_Prepare", maxFetchEnd, adtCommitBegin.getTimestamp()));
            lst.add(genTraceRecord("Distributed_Transaction_Commit", adtCommitBegin.getTimestamp(), adtCommitEnd.getTimestamp()));
        }
        lst.add(genTraceRecord("Write_to_Client", minFetchStart, veryEnd));
        return false;
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

    public boolean isCompleted() {
        return veryStart != 0 && veryEnd != 0 && connFlagMap.size() != 0 && connReceivedMap.size() == connFinishedMap.size() && recordStartMap.size() == recordEndMap.size();
    }

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
        } else if (builder != null) {
            if (genComplexQueryLogs(lst)) return null;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not support trace this query");
            }
            return null;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end genLogResult");
        }
        return lst;
    }


    private boolean genComplexQueryLogs(List<String[]> lst) {
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<MySQLConnection, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<MySQLConnection, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
                if (fetchStartRecordMap == null || fetchEndRecordMap == null || fetchStartRecordMap.size() != 1 || fetchEndRecordMap.size() != 1) {
                    printNoResultDebug(fetchStartRecordMap, fetchEndRecordMap);
                    return true;
                }
                TraceRecord fetchStartRecord = fetchStartRecordMap.values().iterator().next();
                TraceRecord fetchEndRecord = fetchEndRecordMap.values().iterator().next();
                if (!result.isNestLoopQuery()) {
                    lst.add(genLogRecord(result.getName() + "_First_Result_Fetch", lastChildFinished, fetchStartRecord.getTimestamp()));
                } else {
                    TraceRecord handlerStart = recordStartMap.get(handler);
                    TraceRecord handlerEnd = recordEndMap.get(handler);
                    if (handlerStart == null || handlerEnd == null) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("collect info not in pairs for handler" + handler);
                        }
                        return true;
                    }
                    lst.add(genLogRecord("Generate_New_Query", lastChildFinished, handlerStart.getTimestamp()));
                    lst.add(genLogRecord(result.getName() + "_First_Result_Fetch", handlerStart.getTimestamp(), handlerEnd.getTimestamp()));
                }
                lst.add(genLogRecord(result.getName() + "_Last_Result_Fetch", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp()));
            } else if (handler instanceof OutputHandler) {
                TraceRecord startWrite = recordStartMap.get(handler);
                if (startWrite == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for OutputHandler");
                    }
                    return true;
                }
                lst.add(genLogRecord("Write_Client", startWrite.getTimestamp(), veryEnd));
            } else {
                TraceRecord handlerStart = recordStartMap.get(handler);
                TraceRecord handlerEnd = recordEndMap.get(handler);
                if (handlerStart == null || handlerEnd == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for handler" + handler);
                    }
                    return true;
                }
                if (handler.getNextHandler() == null) {
                    lastChildFinished = Math.max(lastChildFinished, handlerEnd.getTimestamp());
                }
            }
        }
        return false;
    }

    private boolean genSimpleLogs(List<String[]> lst) {
        Map<MySQLConnection, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<MySQLConnection, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<MySQLConnection, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
            TraceRecord fetchStartRecord = fetchStart.getValue();
            minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
            executeList.add(genLogRecord(fetchStartRecord.getDataNode() + "_First_Result_Fetch", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp()));
            TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
            if (fetchEndRecord == null) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genLogRecord(fetchStartRecord.getDataNode() + "_Last_Result_Fetch", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp()));
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

    public double getOverAllMilliSecond() {
        return (double) (veryEnd - veryStart) / 1000000;
    }

    private String nanoToSecond(long nano) {
        double milliSecond = (double) nano / 1000000000;
        return String.format("%.6f", milliSecond);
    }

    public String getOverAllSecond() {
        return nanoToSecond(veryEnd - veryStart);
    }

    @Override
    public Object clone() {
        TraceResult tr;
        try {
            tr = (TraceResult) super.clone();
            tr.simpleHandler = this.simpleHandler;
            tr.builder = this.builder;
            tr.connFlagMap = new ConcurrentHashMap<>();
            tr.connFlagMap.putAll(this.connFlagMap);
            tr.connReceivedMap = new ConcurrentHashMap<>();
            for (Map.Entry<ResponseHandler, Map<MySQLConnection, TraceRecord>> item : connReceivedMap.entrySet()) {
                Map<MySQLConnection, TraceRecord> connMap = new ConcurrentHashMap<>();
                connMap.putAll(item.getValue());
                tr.connReceivedMap.put(item.getKey(), connMap);
            }
            tr.connFinishedMap = new ConcurrentHashMap<>();
            for (Map.Entry<ResponseHandler, Map<MySQLConnection, TraceRecord>> item : connFinishedMap.entrySet()) {
                Map<MySQLConnection, TraceRecord> connMap = new ConcurrentHashMap<>();
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
