/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.trace;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.BaseSelectHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.oceanbase.obsharding_d.plan.util.ComplexQueryPlanUtil;
import com.oceanbase.obsharding_d.plan.util.ReferenceHandlerInfo;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.status.SlowQueryLog;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TraceResult implements Cloneable {


    public enum SqlTraceType {
        SINGLE_NODE_QUERY, MULTI_NODE_QUERY, MULTI_NODE_GROUP, COMPLEX_QUERY, SIMPLE_QUERY, COMPLEX_MODIFY
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceResult.class);
    private TraceRecord requestStart;
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
    private ConcurrentMap<ResponseHandler, Map<String, TraceRecord>> connReceivedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<ResponseHandler, Map<String, TraceRecord>> connFinishedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<DMLResponseHandler, TraceRecord> recordStartMap = new ConcurrentHashMap<>();
    private ConcurrentMap<DMLResponseHandler, TraceRecord> recordEndMap = new ConcurrentHashMap<>();

    private SqlTraceType type;
    private TraceRecord requestEnd;
    private boolean subQuery = false;
    private TraceResult previous = null;

    public void setRequestTime() {
        copyToPrevious();
        reset();
        this.requestStart = TraceRecord.currenTime();
    }

    public void startProcess() {
        this.parseStart = TraceRecord.currenTime();
    }

    public void endParse() {
        this.routeStart = TraceRecord.currenTime();
    }

    public void endRoute() {
        this.preExecuteStart = TraceRecord.currenTime();
    }

    public void setPreExecuteEnd(SqlTraceType type0) {
        if (routeStart != null && preExecuteStart != null)
            this.type = type0;
        this.preExecuteEnd = TraceRecord.currenTime();
        clearConnReceivedMap();
        clearConnFlagMap();
    }

    public void setSubQuery() {
        this.subQuery = true;
    }

    public void setBackendResponseTime(MySQLResponseService service, long responseTime) {
        RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
        String key = service.getConnection().getId() + ":" + node.getName() + ":" + +node.getStatementHash();
        ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null && addToConnFlagMap(key) == null) {
            TraceRecord record = new TraceRecord(responseTime, node.getName(), node.getStatement());
            Map<String, TraceRecord> connMap = new ConcurrentHashMap<>();
            connMap.put(key, record);
            addToConnReceivedMap(responseHandler, connMap);
        }
    }

    public void setBackendResponseEndTime(MySQLResponseService service) {
        RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
        ResponseHandler responseHandler = service.getResponseHandler();
        if (responseHandler != null) {
            TraceRecord record = new TraceRecord(System.nanoTime(), node.getName(), node.getStatement());
            Map<String, TraceRecord> connMap = new ConcurrentHashMap<>();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + +node.getStatementHash();
            connMap.put(key, record);
            addToConnFinishedMap(responseHandler, connMap);
        }
    }

    public void setResponseTime(final ShardingService shardingService, boolean isSuccess) {
        if (this.requestEnd == null) {
            this.requestEnd = TraceRecord.currenTime();
            if (SlowQueryLog.getInstance().isEnableSlowLog() && this.isCompletedV1() &&
                    isSuccess && getOverAllMilliSecond() > SlowQueryLog.getInstance().getSlowTime()) {
                SlowQueryLog.getInstance().putSlowQueryLog(shardingService, this.clone());
            }
        }

    }

    public void setShardingNodes(RouteResultsetNode[] shardingNodes) {
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
        this.simpleHandler = simpleHandler;
    }

    public void setBuilder(BaseHandlerBuilder builder) {
        this.builder = builder;
    }

    public void setAdtCommitBegin() {
        this.adtCommitBegin = TraceRecord.currenTime();
    }

    public void setAdtCommitEnd() {
        this.adtCommitEnd = TraceRecord.currenTime();
    }

    private Boolean addToConnFlagMap(String item) {
        return connFlagMap.putIfAbsent(item, true);
    }

    private void clearConnFlagMap() {
        connFlagMap.clear();
    }

    private void addToConnReceivedMap(ResponseHandler responseHandler, Map<String, TraceRecord> connMap) {
        Map<String, TraceRecord> existReceivedMap = connReceivedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    private void clearConnReceivedMap() {
        connReceivedMap.clear();
    }

    public void addToConnFinishedMap(ResponseHandler responseHandler, Map<String, TraceRecord> connMap) {
        Map<String, TraceRecord> existReceivedMap = connFinishedMap.putIfAbsent(responseHandler, connMap);
        if (existReceivedMap != null) {
            existReceivedMap.putAll(connMap);
        }
    }

    public void addToRecordStartMap(DMLResponseHandler handler) {
        recordStartMap.putIfAbsent(handler, TraceRecord.currenTime());
    }

    public void addToRecordEndMap(DMLResponseHandler handler) {
        recordEndMap.putIfAbsent(handler, TraceRecord.currenTime());
    }

    private void reset() {
        requestStart = null;
        requestEnd = null;
        parseStart = null;
        routeStart = null;
        preExecuteStart = null;
        preExecuteEnd = null;
        shardingNodes = null;
        adtCommitBegin = null;
        adtCommitEnd = null;
        type = null;
        subQuery = false;
        simpleHandler = null;
        builder = null; //for complex query
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
    }

    private void copyToPrevious() {
        this.previous = this.clone();
        if (!previous.isCompletedV1()) {
            previous = null;
        }
    }

    // show @@connection.sql.status where FRONT_ID=?
    public List<String[]> genRunningSQLStage() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start genRunningSQLStage");
        }
        List<String[]> lst = new ArrayList<>();
        if (requestStart != null) {
            if (genTraceRecord(lst, "Read_SQL", requestStart, parseStart))
                return lst;
            if (genTraceRecord(lst, "Parse_SQL", parseStart, routeStart, requestEnd))
                return lst;
            if (genTraceRecord(lst, "Route_Calculation", routeStart, preExecuteStart))
                return lst;
            if (genTraceRecord(lst, "Prepare_to_Push/Optimize", preExecuteStart, preExecuteEnd))
                return lst;
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
            }
        }
        return lst;
    }

    private void genRunningSimpleResults(List<String[]> lst) {
        Map<String, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);

        Set<String> receivedNode = new HashSet<>();
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        if (connFetchStartMap != null) {
            Map<String, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
            List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
            List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
            for (Map.Entry<String, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
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
                lst.add(genTraceRecord("Execute_SQL", preExecuteEnd.getTimestamp(), shardingNode.getName(), shardingNode.getStatement()));
                lst.add(genTraceRecord("Fetch_result", shardingNode.getName(), shardingNode.getStatement()));
            }
        }
        if (adtCommitBegin != null) {
            lst.add(genTraceRecord("Distributed_Transaction_Prepare", maxFetchEnd, adtCommitBegin.getTimestamp()));
            lst.add(genTraceRecord("Distributed_Transaction_Commit", adtCommitBegin.getTimestamp(), adtCommitEnd.getTimestamp()));
        }
        if (minFetchStart == Long.MAX_VALUE) {
            lst.add(genTraceRecord("Write_to_Client"));
        } else if (requestEnd == null) {
            lst.add(genTraceRecord("Write_to_Client", minFetchStart));
        } else {
            lst.add(genTraceRecord("Write_to_Client", minFetchStart, requestEnd.getTimestamp()));
        }
    }

    private void genRunningComplexQueryResults(List<String[]> lst) {
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<String, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
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
                    Map<String, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
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
                } else if (requestEnd == null) {
                    lst.add(genTraceRecord("Write_to_Client", startWrite.getTimestamp()));
                } else {
                    lst.add(genTraceRecord("Write_to_Client", startWrite.getTimestamp(), requestEnd.getTimestamp()));
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

    // show trace
    public List<String[]> genShowTraceResult() {
        try {
            if (this.previous != null) {
                return this.previous.genTraceResult();
            }
            return null;
        } catch (Exception e) {
            LOGGER.warn("genShowTraceResult exception {}", e);
            return null;
        } finally {
            this.previous = null;
        }
    }

    private List<String[]> genTraceResult() {
        List<String[]> lst = new ArrayList<>();
        if (isCompletedV2()) {
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
        } else {
            if (isCompletedV1() && isNonBusinessSql()) {
                genTraceRecord(lst, "Read_SQL", requestStart, parseStart);
                genTraceRecord(lst, "Parse_SQL", parseStart, routeStart, requestEnd);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("collect info not in pairs,requestEnd:" + requestEnd.getTimestamp() + ",connFlagMap.size:" + connFlagMap.size() +
                            ",connReceivedMap.size:" + connReceivedMap.size() + ",connFinishedMap.size:" + connFinishedMap.size() +
                            ",recordStartMap.size:" + recordStartMap.size() + ",recordEndMap.size:" + recordEndMap.size());
                }
                return null;
            }
        }
        if (lst.size() > 0) {
            lst.add(genTraceRecord("Over_All", requestStart.getTimestamp(), requestEnd.getTimestamp()));
        }
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
                Map<String, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<String, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
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
                lst.add(genTraceRecord("Write_to_Client", startWrite.getTimestamp(), requestEnd.getTimestamp()));
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

    private boolean genSimpleResults(List<String[]> lst) {
        lst.add(genTraceRecord("Route_Calculation", routeStart.getTimestamp(), preExecuteStart.getTimestamp()));
        lst.add(genTraceRecord("Prepare_to_Push", preExecuteStart.getTimestamp(), preExecuteEnd.getTimestamp()));
        Map<String, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<String, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<String, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
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
        lst.add(genTraceRecord("Write_to_Client", minFetchStart, requestEnd.getTimestamp()));
        return false;
    }

    // slow log
    public List<String[]> genLogResult() {
        List<String[]> lst = new ArrayList<>();
        if (isCompletedV2()) {
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
        } else {
            if (isCompletedV1() && isNonBusinessSql()) {
                lst.add(genLogRecord("Read_SQL", requestStart.getTimestamp(), parseStart.getTimestamp()));
                lst.add(genLogRecord("Inner_Execute", parseStart.getTimestamp(), requestEnd.getTimestamp()));
                lst.add(genLogRecord("Write_Client", requestEnd.getTimestamp(), requestEnd.getTimestamp()));
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("collect info not in pairs,requestEnd:" + requestEnd.getTimestamp() + ",connFlagMap.size:" + connFlagMap.size() +
                            ",connReceivedMap.size:" + connReceivedMap.size() + ",connFinishedMap.size:" + connFinishedMap.size() +
                            ",recordStartMap.size:" + recordStartMap.size() + ",recordEndMap.size:" + recordEndMap.size());
                }
                return null;
            }
        }
        return lst;
    }

    private boolean genComplexQueryLogs(List<String[]> lst) {
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(builder);
        long lastChildFinished = preExecuteEnd.getTimestamp();
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                Map<String, TraceRecord> fetchStartRecordMap = connReceivedMap.get(handler);
                Map<String, TraceRecord> fetchEndRecordMap = connFinishedMap.get(handler);
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
                lst.add(genLogRecord("Write_Client", startWrite.getTimestamp(), requestEnd.getTimestamp()));
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
        Map<String, TraceRecord> connFetchStartMap = connReceivedMap.get(simpleHandler);
        Map<String, TraceRecord> connFetchEndMap = connFinishedMap.get(simpleHandler);
        List<String[]> executeList = new ArrayList<>(connFetchStartMap.size());
        List<String[]> fetchList = new ArrayList<>(connFetchStartMap.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (Map.Entry<String, TraceRecord> fetchStart : connFetchStartMap.entrySet()) {
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
        lst.add(genLogRecord("Write_Client", minFetchStart, requestEnd.getTimestamp()));
        return false;
    }

    private boolean isCompletedV1() {
        return requestStart != null && requestEnd != null;
    }

    public boolean isCompletedV2() {
        return isCompletedV1() && routeStart != null && connFlagMap.size() != 0 && connReceivedMap.size() == connFinishedMap.size() && recordStartMap.size() == recordEndMap.size();
    }

    public boolean isNonBusinessSql() {
        return type == null; // || routeStart == null;
    }

    private boolean genTraceRecord(List<String[]> lst, String operation, TraceRecord start0, TraceRecord end0) {
        return genTraceRecord(lst, operation, start0, end0, null);
    }

    private boolean genTraceRecord(List<String[]> lst, String operation, TraceRecord start0, TraceRecord end0, TraceRecord finalEnd) {
        if (end0 == null) {
            if (finalEnd != null) {
                lst.add(genTraceRecord(operation, start0.getTimestamp(), finalEnd.getTimestamp()));
                lst.add(genTraceRecord("Write_to_Client", finalEnd.getTimestamp(), finalEnd.getTimestamp()));
            } else {
                lst.add(genTraceRecord(operation, start0.getTimestamp()));
            }
            return true;
        } else {
            lst.add(genTraceRecord(operation, start0.getTimestamp(), end0.getTimestamp()));
            return false;
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
        readQuery[1] = nanoToMilliSecond(start - requestStart.getTimestamp());
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
        readQuery[1] = nanoToMilliSecond(start - requestStart.getTimestamp());
        readQuery[2] = nanoToMilliSecond(end - requestStart.getTimestamp());
        readQuery[3] = nanoToMilliSecond(end - start);
        readQuery[4] = shardingNode;
        readQuery[5] = ref.replaceAll("[\\t\\n\\r]", " ");
        return readQuery;
    }

    private void printNoResultDebug(Map<String, TraceRecord> fetchStartRecordMap, Map<String, TraceRecord> fetchEndRecordMap) {
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

    private String[] genLogRecord(String operation, long start, long end) {
        String[] readQuery = new String[2];
        readQuery[0] = operation;
        readQuery[1] = nanoToSecond(end - start);
        return readQuery;
    }

    public double getOverAllMilliSecond() {
        return (double) (requestEnd.getTimestamp() - requestStart.getTimestamp()) / 1000000;
    }

    public String getOverAllSecond() {
        return nanoToSecond(requestEnd.getTimestamp() - requestStart.getTimestamp());
    }

    private String nanoToMilliSecond(long nano) {
        double milliSecond = (double) nano / 1000000;
        return String.valueOf(milliSecond);
    }

    private String nanoToSecond(long nano) {
        double milliSecond = (double) nano / 1000000000;
        return String.format("%.6f", milliSecond);
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

    @Override
    public TraceResult clone() {
        TraceResult tr;
        try {
            tr = (TraceResult) super.clone();
            tr.previous = null;
            tr.simpleHandler = this.simpleHandler;
            tr.builder = this.builder;
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
