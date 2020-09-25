/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.BaseSelectHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.plan.util.ComplexQueryPlanUtil;
import com.actiontech.dble.plan.util.ReferenceHandlerInfo;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TraceResult implements Cloneable {


    public enum SqlTraceType {
        SINGLE_NODE_QUERY, MULTI_NODE_QUERY, MULTI_NODE_GROUP, COMPLEX_QUERY;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceResult.class);
    private boolean prepareFinished = false;
    private long veryStartPrepare;
    private long veryStart;
    private TraceRecord requestStartPrepare;
    private TraceRecord requestStart;
    private TraceRecord parseStartPrepare; //requestEnd
    private TraceRecord parseStart; //requestEnd
    private TraceRecord routeStart; //parseEnd
    private TraceRecord preExecuteStart; //routeEnd
    private RouteResultsetNode[] shardingNodes;
    private TraceRecord preExecuteEnd;

    private TraceRecord adtCommitBegin; //auto Distributed Transaction commit begin
    private TraceRecord adtCommitEnd; ////auto Distributed Transaction commit end

    private ResponseHandler simpleHandler = null;
    private BaseHandlerBuilder builder = null; //for complex query
    private ConcurrentMap<String, Boolean> connFlagMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<MySQLResponseService, TraceRecord>> connReceivedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<MySQLResponseService, TraceRecord>> connFinishedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<DMLResponseHandler, TraceRecord> recordStartMap = new ConcurrentHashMap<>();
    private ConcurrentMap<DMLResponseHandler, TraceRecord> recordEndMap = new ConcurrentHashMap<>();
    private long veryEnd;
    private SqlTraceType type;
    private boolean subQuery = false;

    public void setVeryStartPrepare(long veryStartPrepare) {
        prepareFinished = false;
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

    public RouteResultsetNode[] getShardingNodes() {
        return shardingNodes;
    }

    public void setShardingNodes(RouteResultsetNode[] shardingNodes) {
        if (this.shardingNodes == null) {
            this.shardingNodes = shardingNodes;
        } else {
            RouteResultsetNode[] tempShardingNodes = new RouteResultsetNode[this.shardingNodes.length + shardingNodes.length];
            System.arraycopy(this.shardingNodes, 0, tempShardingNodes, 0, this.shardingNodes.length);
            System.arraycopy(shardingNodes, 0, tempShardingNodes, this.shardingNodes.length, shardingNodes.length);
            this.shardingNodes = tempShardingNodes;
        }
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

    public void setType(SqlTraceType type) {
        this.type = type;
    }

    public void setSubQuery(boolean subQuery) {
        this.subQuery = subQuery;
    }

    public Boolean addToConnFlagMap(String item) {
        return connFlagMap.putIfAbsent(item, true);
    }

    public void clearConnFlagMap() {
        connFlagMap.clear();
    }

    public void addToConnReceivedMap(ResponseHandler responseHandler, Map<MySQLResponseService, TraceRecord> connMap) {
        Map<MySQLResponseService, TraceRecord> existReceivedMap = connReceivedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public void clearConnReceivedMap() {
        connReceivedMap.clear();
    }

    public void addToConnFinishedMap(ResponseHandler responseHandler, Map<MySQLResponseService, TraceRecord> connMap) {
        Map<MySQLResponseService, TraceRecord> existReceivedMap = connFinishedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public void addToRecordStartMap(DMLResponseHandler handler, TraceRecord traceRecord) {
        recordStartMap.putIfAbsent(handler, traceRecord);
    }

    public void addToRecordEndMap(DMLResponseHandler handler, TraceRecord traceRecord) {
        recordEndMap.putIfAbsent(handler, traceRecord);
    }

    public void setVeryEnd(long veryEnd) {
        this.veryEnd = veryEnd;
    }

    public void ready() {
        prepareFinished = true;
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
        routeStart = null;
        preExecuteStart = null;
        preExecuteEnd = null;
        shardingNodes = null;
        adtCommitBegin = null;
        adtCommitEnd = null;
        this.type = null;
        subQuery = false;
        simpleHandler = null;
        builder = null; //for complex query
        connFlagMap.clear();
        for (Map<MySQLResponseService, TraceRecord> connReceived : connReceivedMap.values()) {
            connReceived.clear();
        }
        connReceivedMap.clear();
        for (Map<MySQLResponseService, TraceRecord> connReceived : connFinishedMap.values()) {
            connReceived.clear();
        }
        connFinishedMap.clear();
        recordStartMap.clear();
        recordEndMap.clear();
        veryEnd = 0;
    }

    public List<String[]> genRunningSQLStage() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start genRunningSQLStage");
        }
        List<String[]> lst = new ArrayList<>();
        if (!prepareFinished) {
            if (requestStartPrepare == null) {
                return lst;
            } else {
                if (parseStartPrepare == null) {
                    lst.add(genTraceRecord("Read_SQL", requestStartPrepare.getTimestamp()));
                    return lst;
                } else {
                    lst.add(genTraceRecord("Read_SQL", requestStartPrepare.getTimestamp(), parseStartPrepare.getTimestamp()));
                    lst.add(genTraceRecord("Parse_SQL", parseStartPrepare.getTimestamp()));
                    return lst;
                }
            }
        }
        lst.add(genTraceRecord("Read_SQL", requestStart.getTimestamp(), parseStart.getTimestamp()));

        if (routeStart == null) {
            lst.add(genTraceRecord("Parse_SQL", parseStart.getTimestamp()));
            return lst;
        } else {
            lst.add(genTraceRecord("Parse_SQL", parseStart.getTimestamp(), routeStart.getTimestamp()));
        }

        if (preExecuteStart == null) {
            lst.add(genTraceRecord("Route_Calculation", routeStart.getTimestamp()));
            return lst;
        } else {
            lst.add(genTraceRecord("Route_Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        }

        if (preExecuteEnd == null) {
            lst.add(genTraceRecord("Prepare_to_Push/Optimize", preExecuteStart.getTimestamp()));
            return lst;
        } else {
            lst.add(genTraceRecord("Prepare_to_Push/Optimize", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        }
        if (simpleHandler != null) {
            genRunningSimpleResults(lst);
            return lst;
        } else if (builder != null) {
            genRunningComplexQueryResults(lst);
            return lst;
        } else if (subQuery) {
            lst.add(genTraceRecord("Doing_SubQuery", preExecuteEnd.getTimestamp()));
            return lst;
        } else if (shardingNodes == null || (this.type == SqlTraceType.COMPLEX_QUERY)) {
            lst.add(genTraceRecord("Generate_Query_Explain", preExecuteEnd.getTimestamp()));
            return lst;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not support trace this query or unfinished");
            }
            return lst;
        }
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

    private void genRunningComplexQueryResults(List<String[]> lst) {
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<MySQLResponseService, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                if (fetchStartRecordMap == null) {
                    if (!result.isNestLoopQuery()) {
                        lst.add(genTraceRecord("Execute_SQL", lastChildFinished, result.getName(), result.getRefOrSQL())); // lastChildFinished may is Long.MAX_VALUE
                    } else {
                        lst.add(genTraceRecord("Generate_New_Query", lastChildFinished)); // lastChildFinished may is Long.MAX_VALUE
                    }
                    lst.add(genTraceRecord("Fetch_result", result.getName(), result.getRefOrSQL()));
                } else {
                    TraceRecord fetchStartRecord = fetchStartRecordMap.values().iterator().next();
                    if (!result.isNestLoopQuery()) {
                        lst.add(genTraceRecord("Execute_SQL", lastChildFinished, fetchStartRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
                    } else {
                        TraceRecord handlerStart = recordStartMap.get(handler);
                        TraceRecord handlerEnd = recordEndMap.get(handler);
                        if (handlerStart == null) {
                            lst.add(genTraceRecord("Generate_New_Query", lastChildFinished)); // lastChildFinished may is Long.MAX_VALUE
                        } else if (handlerEnd == null) {
                            lst.add(genTraceRecord("Generate_New_Query", lastChildFinished, handlerStart.getTimestamp()));
                            lst.add(genTraceRecord("Execute_SQL", handlerStart.getTimestamp(), result.getName(), result.getRefOrSQL()));
                        } else {
                            lst.add(genTraceRecord("Generate_New_Query", lastChildFinished, handlerStart.getTimestamp()));
                            lst.add(genTraceRecord("Execute_SQL", handlerStart.getTimestamp(), handlerEnd.getTimestamp(), result.getName(), result.getRefOrSQL()));
                        }
                    }
                    Map<MySQLResponseService, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
                    if (fetchEndRecordMap == null) {
                        lst.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
                    } else {
                        TraceRecord fetchEndRecord = fetchEndRecordMap.values().iterator().next();
                        lst.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), result.getName(), result.getRefOrSQL()));
                    }
                }
            } else if (handler instanceof OutputHandler) {
                TraceRecord startWrite = recordStartMap.get(handler);
                if (startWrite == null) {
                    lst.add(genTraceRecord("Write_to_Client"));
                } else if (veryEnd == 0) {
                    lst.add(genTraceRecord("Write_to_Client", startWrite.getTimestamp()));
                } else {
                    lst.add(genTraceRecord("Write_to_Client", startWrite.getTimestamp(), veryEnd));
                }
            } else {
                TraceRecord handlerStart = recordStartMap.get(handler);
                TraceRecord handlerEnd = recordEndMap.get(handler);
                if (handlerStart == null) {
                    lst.add(genTraceRecord(result.getType()));
                } else if (handlerEnd == null) {
                    lst.add(genTraceRecord(result.getType(), handlerStart.getTimestamp(), result.getName(), result.getRefOrSQL()));
                } else {
                    lst.add(genTraceRecord(result.getType(), handlerStart.getTimestamp(), handlerEnd.getTimestamp(), result.getName(), result.getRefOrSQL()));
                }

                if (handler.getNextHandler() == null) {
                    if (handlerEnd != null) {
                        lastChildFinished = Math.max(lastChildFinished, handlerEnd.getTimestamp());
                    } else {
                        lastChildFinished = Long.MAX_VALUE;
                    }
                }
            }
        }
    }

    private boolean genComplexQueryResults(List<String[]> lst) {
        lst.add(genTraceRecord("Try_Route_Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Try_to_Optimize", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<MySQLResponseService, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<MySQLResponseService, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
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

    private void printNoResultDebug(Map<MySQLResponseService, TraceRecord> fetchStartRecordMap, Map<MySQLResponseService, TraceRecord> fetchEndRecordMap) {
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
        Map<MySQLResponseService, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<MySQLResponseService, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<MySQLResponseService, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
            TraceRecord fetchStartRecord = fetchStart.getValue();
            minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
            executeList.add(genTraceRecord("Execute_SQL", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp(), fetchStartRecord.getShardingNode(), fetchStartRecord.getRef()));
            TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
            if (fetchEndRecord == null) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), fetchStartRecord.getShardingNode(), fetchStartRecord.getRef()));
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

    private void genRunningSimpleResults(List<String[]> lst) {
        Map<MySQLResponseService, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);

        Set<String> receivedNode = new HashSet<>();
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        if (connFetchStartMap != null) {
            Map<MySQLResponseService, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
            List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
            List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
            for (Map.Entry<MySQLResponseService, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
                TraceRecord fetchStartRecord = fetchStart.getValue();
                receivedNode.add(fetchStartRecord.getShardingNode());
                minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
                executeList.add(genTraceRecord("Execute_SQL", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp(), fetchStartRecord.getShardingNode(), fetchStartRecord.getRef()));
                if (connFetchEndMap == null) {
                    fetchList.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchStartRecord.getShardingNode(), fetchStartRecord.getRef()));
                } else {
                    TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
                    if (fetchEndRecord == null) {
                        fetchList.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchStartRecord.getShardingNode(), fetchStartRecord.getRef()));
                    } else {
                        fetchList.add(genTraceRecord("Fetch_result", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp(), fetchStartRecord.getShardingNode(), fetchStartRecord.getRef()));
                        maxFetchEnd = Math.max(maxFetchEnd, fetchEndRecord.getTimestamp());
                    }
                }
            }
            lst.addAll(executeList);
            if (receivedNode.size() != shardingNodes.length) {
                for (RouteResultsetNode shardingNode : shardingNodes) {
                    if (!receivedNode.contains(shardingNode.getName())) {
                        lst.add(genTraceRecord("Execute_SQL", preExecuteEnd.getTimestamp(), shardingNode.getName(), shardingNode.getStatement()));
                        fetchList.add(genTraceRecord("Fetch_result", shardingNode.getName(), shardingNode.getStatement()));
                    }
                }
            }
            lst.addAll(fetchList);
        } else {
            for (RouteResultsetNode shardingNode : shardingNodes) {
                if (!receivedNode.contains(shardingNode.getName())) {
                    lst.add(genTraceRecord("Execute_SQL", preExecuteEnd.getTimestamp(), shardingNode.getName(), shardingNode.getStatement()));
                    lst.add(genTraceRecord("Fetch_result", shardingNode.getName(), shardingNode.getStatement()));
                }
            }
        }
        if (adtCommitBegin != null) {
            lst.add(genTraceRecord("Distributed_Transaction_Prepare", maxFetchEnd, adtCommitBegin.getTimestamp()));
            lst.add(genTraceRecord("Distributed_Transaction_Commit", adtCommitBegin.getTimestamp(), adtCommitEnd.getTimestamp()));
        }
        if (minFetchStart == Long.MAX_VALUE) {
            lst.add(genTraceRecord("Write_to_Client"));
        } else if (veryEnd == 0) {
            lst.add(genTraceRecord("Write_to_Client", minFetchStart));
        } else {
            lst.add(genTraceRecord("Write_to_Client", minFetchStart, veryEnd));
        }
    }

    private String[] genTraceRecord(String operation, long start) {
        return genTraceRecord(operation, start, "-", "-");

    }

    private String[] genTraceRecord(String operation, long start, String shardingNode, String ref) {
        if (start == Long.MAX_VALUE) {
            return genTraceRecord(operation, shardingNode, ref);
        }
        String[] readQuery = new String[6];
        readQuery[0] = operation;
        readQuery[1] = nanoToMilliSecond(start - veryStart);
        readQuery[2] = "unfinished";
        readQuery[3] = "unknown";
        readQuery[4] = shardingNode;
        readQuery[5] = ref.replaceAll("[\\t\\n\\r]", " ");
        return readQuery;
    }


    private String[] genTraceRecord(String operation, String shardingNode, String ref) {
        String[] readQuery = new String[6];
        readQuery[0] = operation;
        readQuery[1] = "not started";
        readQuery[2] = "unfinished";
        readQuery[3] = "unknown";
        readQuery[4] = shardingNode;
        readQuery[5] = ref.replaceAll("[\\t\\n\\r]", " ");
        return readQuery;
    }

    private String[] genTraceRecord(String operation) {
        return genTraceRecord(operation, "-", "-");
    }

    private String[] genTraceRecord(String operation, long start, long end) {
        return genTraceRecord(operation, start, end, "-", "-");
    }

    private String[] genTraceRecord(String operation, long start, long end, String shardingNode, String ref) {
        String[] readQuery = new String[6];
        readQuery[0] = operation;
        readQuery[1] = nanoToMilliSecond(start - veryStart);
        readQuery[2] = nanoToMilliSecond(end - veryStart);
        readQuery[3] = nanoToMilliSecond(end - start);
        readQuery[4] = shardingNode;
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

    public SqlTraceType getType() {
        return this.type;
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
                Map<MySQLResponseService, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<MySQLResponseService, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
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
        Map<MySQLResponseService, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<MySQLResponseService, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<MySQLResponseService, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
            TraceRecord fetchStartRecord = fetchStart.getValue();
            minFetchStart = Math.min(minFetchStart, fetchStartRecord.getTimestamp());
            executeList.add(genLogRecord(fetchStartRecord.getShardingNode() + "_First_Result_Fetch", preExecuteEnd.getTimestamp(), fetchStartRecord.getTimestamp()));
            TraceRecord fetchEndRecord = connFetchEndMap.get(fetchStart.getKey());
            if (fetchEndRecord == null) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genLogRecord(fetchStartRecord.getShardingNode() + "_Last_Result_Fetch", fetchStartRecord.getTimestamp(), fetchEndRecord.getTimestamp()));
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
            for (Map.Entry<ResponseHandler, Map<MySQLResponseService, TraceRecord>> item : connReceivedMap.entrySet()) {
                Map<MySQLResponseService, TraceRecord> connMap = new ConcurrentHashMap<>();
                connMap.putAll(item.getValue());
                tr.connReceivedMap.put(item.getKey(), connMap);
            }
            tr.connFinishedMap = new ConcurrentHashMap<>();
            for (Map.Entry<ResponseHandler, Map<MySQLResponseService, TraceRecord>> item : connFinishedMap.entrySet()) {
                Map<MySQLResponseService, TraceRecord> connMap = new ConcurrentHashMap<>();
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
