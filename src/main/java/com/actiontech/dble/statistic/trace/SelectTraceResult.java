package com.actiontech.dble.statistic.trace;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.BaseSelectHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.plan.util.ComplexQueryPlanUtil;
import com.actiontech.dble.plan.util.ReferenceHandlerInfo;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectTraceResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectTraceResult.class);
    final TraceResult tr;

    public SelectTraceResult(TraceResult traceResult) {
        this.tr = traceResult;
    }

    // show @@connection.sql.status where FRONT_ID=?
    public List<String[]> genRunningSQLStage() {
        if (!tr.isDetailTrace) return null;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start genRunningSQLStage");
        }
        List<String[]> lst = new ArrayList<>();
        if (tr.requestStart != 0) {
            if (genTraceRecord(lst, "Read_SQL", tr.requestStart, tr.parseStart))
                return lst;
            if (genTraceRecord(lst, "Parse_SQL", tr.parseStart, tr.routeStart, tr.requestEnd))
                return lst;
            if (genTraceRecord(lst, "Route_Calculation", tr.routeStart, tr.preExecuteStart))
                return lst;
            if (genTraceRecord(lst, "Prepare_to_Push/Optimize", tr.preExecuteStart, tr.preExecuteEnd))
                return lst;
            if (tr.simpleHandler != null) {
                genRunningSimpleResults(lst);
                return lst;
            } else if (tr.builder != null) {
                genRunningComplexQueryResults(lst);
                return lst;
            } else if (tr.subQuery) {
                lst.add(genTraceRecord("Doing_SubQuery", tr.preExecuteEnd));
                return lst;
            } else if (tr.shardingNodes == null || (tr.type == TraceResult.SqlTraceType.COMPLEX_QUERY)) {
                lst.add(genTraceRecord("Generate_Query_Explain", tr.preExecuteEnd));
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
        List<TraceResult.BackendRoute> bRs = tr.findByHandler(tr.simpleHandler);
        List<String[]> executeList = new ArrayList<>(bRs.size());
        List<String[]> fetchList = new ArrayList<>(bRs.size());

        Set<String> receivedNode = new HashSet<>();
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        if (!CollectionUtil.isEmpty(bRs)) {
            for (TraceResult.BackendRoute ar : bRs) {
                receivedNode.add(ar.getShardingNode());
                minFetchStart = Math.min(minFetchStart, ar.getFirstRevTime());
                executeList.add(genTraceRecord("Execute_SQL", tr.preExecuteEnd, ar.getFirstRevTime(), ar.getShardingNode(), ar.getSql()));
                if (ar.getFinished() == 0) {
                    fetchList.add(genTraceRecord("Fetch_result", ar.getFirstRevTime(), ar.getShardingNode(), ar.getSql()));
                } else {
                    fetchList.add(genTraceRecord("Fetch_result", ar.getFirstRevTime(), ar.getFinished(), ar.getShardingNode(), ar.getSql()));
                    maxFetchEnd = Math.max(maxFetchEnd, ar.getFinished());
                }
            }
            lst.addAll(executeList);
            if (receivedNode.size() != tr.shardingNodes.length) {
                for (RouteResultsetNode shardingNode : tr.shardingNodes) {
                    if (!receivedNode.contains(shardingNode.getName())) {
                        lst.add(genTraceRecord("Execute_SQL", tr.preExecuteEnd, shardingNode.getName(), shardingNode.getStatement()));
                        fetchList.add(genTraceRecord("Fetch_result", shardingNode.getName(), shardingNode.getStatement()));
                    }
                }
            }
            lst.addAll(fetchList);
        } else {
            for (RouteResultsetNode shardingNode : tr.shardingNodes) {
                lst.add(genTraceRecord("Execute_SQL", tr.preExecuteEnd, shardingNode.getName(), shardingNode.getStatement()));
                lst.add(genTraceRecord("Fetch_result", shardingNode.getName(), shardingNode.getStatement()));
            }
        }
        if (tr.adtCommitBegin != 0) {
            lst.add(genTraceRecord("Distributed_Transaction_Prepare", maxFetchEnd, tr.adtCommitBegin));
            lst.add(genTraceRecord("Distributed_Transaction_Commit", tr.adtCommitBegin, tr.adtCommitEnd));
        }
        if (minFetchStart == Long.MAX_VALUE) {
            lst.add(genTraceRecord("Write_to_Client"));
        } else if (tr.requestEnd == 0) {
            lst.add(genTraceRecord("Write_to_Client", minFetchStart));
        } else {
            lst.add(genTraceRecord("Write_to_Client", minFetchStart, tr.requestEnd));
        }
    }

    private void genRunningComplexQueryResults(List<String[]> lst) {
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(tr.builder);
        long lastChildFinished = tr.preExecuteEnd;
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                List<TraceResult.BackendRoute> bRs = tr.findByHandler(handler);
                if (CollectionUtil.isEmpty(bRs)) {
                    if (!result.isNestLoopQuery()) {
                        lst.add(genTraceRecord("Execute_SQL", lastChildFinished, result.getName(), result.getRefOrSQL())); // lastChildFinished may is Long.MAX_VALUE
                    } else {
                        lst.add(genTraceRecord("Generate_New_Query", lastChildFinished)); // lastChildFinished may is Long.MAX_VALUE
                    }
                    lst.add(genTraceRecord("Fetch_result", result.getName(), result.getRefOrSQL()));
                } else {
                    TraceResult.BackendRoute ar = bRs.iterator().next();
                    if (!result.isNestLoopQuery()) {
                        lst.add(genTraceRecord("Execute_SQL", lastChildFinished, ar.getFirstRevTime(), result.getName(), result.getRefOrSQL()));
                    } else {
                        TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                        if (complexHandler == null) {
                            lst.add(genTraceRecord("Generate_New_Query", lastChildFinished)); // lastChildFinished may is Long.MAX_VALUE
                        } else if (complexHandler.getEndTime() == 0) {
                            lst.add(genTraceRecord("Generate_New_Query", lastChildFinished, complexHandler.getStartTime()));
                            lst.add(genTraceRecord("Execute_SQL", complexHandler.getStartTime(), result.getName(), result.getRefOrSQL()));
                        } else {
                            lst.add(genTraceRecord("Generate_New_Query", lastChildFinished, complexHandler.getStartTime()));
                            lst.add(genTraceRecord("Execute_SQL", complexHandler.getStartTime(), complexHandler.getEndTime(), result.getName(), result.getRefOrSQL()));
                        }
                    }
                    if (ar.getFinished() == 0) {
                        lst.add(genTraceRecord("Fetch_result", ar.getFirstRevTime(), result.getName(), result.getRefOrSQL()));
                    } else {
                        lst.add(genTraceRecord("Fetch_result", ar.getFirstRevTime(), ar.getFinished(), result.getName(), result.getRefOrSQL()));
                    }
                }
            } else if (handler instanceof OutputHandler) {
                TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                if (complexHandler == null) {
                    lst.add(genTraceRecord("Write_to_Client"));
                } else if (tr.requestEnd == 0) {
                    lst.add(genTraceRecord("Write_to_Client", complexHandler.getStartTime()));
                } else {
                    lst.add(genTraceRecord("Write_to_Client", complexHandler.getStartTime(), tr.requestEnd));
                }
            } else {
                TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                if (complexHandler == null) {
                    lst.add(genTraceRecord(result.getType()));
                } else if (complexHandler.getEndTime() == 0) {
                    lst.add(genTraceRecord(result.getType(), complexHandler.getStartTime(), result.getName(), result.getRefOrSQL()));
                } else {
                    lst.add(genTraceRecord(result.getType(), complexHandler.getStartTime(), complexHandler.getEndTime(), result.getName(), result.getRefOrSQL()));
                }

                if (handler.getNextHandler() == null) {
                    if (complexHandler.getEndTime() != 0) {
                        lastChildFinished = Math.max(lastChildFinished, complexHandler.getEndTime());
                    } else {
                        lastChildFinished = Long.MAX_VALUE;
                    }
                }
            }
        }
    }

    // show trace
    protected List<String[]> genTraceResult() {
        if (!tr.isDetailTrace) return null;
        List<String[]> lst = new ArrayList<>();
        if (tr.isCompletedV2()) {
            lst.add(genTraceRecord("Read_SQL", tr.requestStart, tr.parseStart));
            lst.add(genTraceRecord("Parse_SQL", tr.parseStart, tr.routeStart));
            if (tr.simpleHandler != null) {
                if (genSimpleResults(lst)) return null;
            } else if (tr.builder != null) {
                if (genComplexQueryResults(lst)) return null;
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("not support trace this query");
                }
                return null;
            }
        } else {
            if (tr.isCompletedV1() && tr.isNonBusinessSql()) {
                genTraceRecord(lst, "Read_SQL", tr.requestStart, tr.parseStart);
                genTraceRecord(lst, "Parse_SQL", tr.parseStart, tr.routeStart, tr.requestEnd);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    long requestCount = tr.backendRouteList.size();
                    long firstRevCount = tr.backendRouteList.stream().filter(f -> f.getFirstRevTime() != 0).count();
                    long finishedCount = tr.backendRouteList.stream().filter(f -> f.getFinished() != 0).count();
                    long recordStartCount = tr.complexHandlerList.size();
                    long recordEndCount = tr.complexHandlerList.stream().filter(f -> f.endTime != 0).count();
                    LOGGER.debug("collect info not in pairs; requestEnd:{}, requestCount:{}, firstRevCount:{}, finishedCount:{}, recordStartCount:{}, recordEndCount:{}",
                            tr.requestEnd, requestCount, firstRevCount, finishedCount, recordStartCount, recordEndCount);
                }
                return null;
            }
        }
        if (lst.size() > 0) {
            lst.add(genTraceRecord("Over_All", tr.requestStart, tr.requestEnd));
        }
        return lst;
    }

    private boolean genComplexQueryResults(List<String[]> lst) {
        lst.add(genTraceRecord("Try_Route_Calculation", tr.routeStart, tr.preExecuteStart));
        lst.add(genTraceRecord("Try_to_Optimize", tr.preExecuteStart, tr.preExecuteEnd));
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(tr.builder);
        long lastChildFinished = tr.preExecuteEnd;
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                List<TraceResult.BackendRoute> bRs = tr.findByHandler(handler);
                long firstRevCount = bRs.stream().filter(f -> f.getFirstRevTime() != 0).count();
                long finishedCount = bRs.stream().filter(f -> f.getFinished() != 0).count();
                if (CollectionUtil.isEmpty(bRs) || firstRevCount != 1 || finishedCount != 1) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("collect info not in pairs for connection; firstRevCount is {}, finishedCount is {}", firstRevCount, finishedCount);
                    return true;
                }
                TraceResult.BackendRoute ar = bRs.iterator().next();
                if (!result.isNestLoopQuery()) {
                    lst.add(genTraceRecord("Execute_SQL", lastChildFinished, ar.getFirstRevTime(), result.getName(), result.getRefOrSQL()));
                } else {
                    TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                    if (complexHandler == null || complexHandler.getEndTime() == 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("collect info not in pairs for handler" + handler);
                        }
                        return true;
                    }
                    lst.add(genTraceRecord("Generate_New_Query", lastChildFinished, complexHandler.getStartTime()));
                    lst.add(genTraceRecord("Execute_SQL", complexHandler.getStartTime(), complexHandler.getEndTime(), result.getName(), result.getRefOrSQL()));
                }
                lst.add(genTraceRecord("Fetch_result", ar.getFirstRevTime(), ar.getFinished(), result.getName(), result.getRefOrSQL()));
            } else if (handler instanceof OutputHandler) {
                TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                if (complexHandler == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for OutputHandler");
                    }
                    return true;
                }
                lst.add(genTraceRecord("Write_to_Client", complexHandler.getStartTime(), tr.requestEnd));
            } else {
                TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                if (complexHandler == null || complexHandler.getEndTime() == 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for handler" + handler);
                    }
                    return true;
                }
                lst.add(genTraceRecord(result.getType(), complexHandler.getStartTime(), complexHandler.getEndTime(), result.getName(), result.getRefOrSQL()));
                if (handler.getNextHandler() == null) {
                    lastChildFinished = Math.max(lastChildFinished, complexHandler.getEndTime());
                }
            }
        }
        return false;
    }

    private boolean genSimpleResults(List<String[]> lst) {
        lst.add(genTraceRecord("Route_Calculation", tr.routeStart, tr.preExecuteStart));
        lst.add(genTraceRecord("Prepare_to_Push", tr.preExecuteStart, tr.preExecuteEnd));
        List<TraceResult.BackendRoute> bRs = tr.findByHandler(tr.simpleHandler);
        List<String[]> executeList = new ArrayList<>(bRs.size());
        List<String[]> fetchList = new ArrayList<>(bRs.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (TraceResult.BackendRoute ar : bRs) {
            minFetchStart = Math.min(minFetchStart, ar.getFirstRevTime());
            executeList.add(genTraceRecord("Execute_SQL", tr.preExecuteEnd, ar.getFirstRevTime(), ar.getShardingNode(), ar.getSql()));
            if (ar.getFinished() == 0) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genTraceRecord("Fetch_result", ar.getFirstRevTime(), ar.getFinished(), ar.getShardingNode(), ar.getSql()));
            maxFetchEnd = Math.max(maxFetchEnd, ar.getFinished());
        }
        lst.addAll(executeList);
        lst.addAll(fetchList);
        if (tr.adtCommitBegin != 0) {
            lst.add(genTraceRecord("Distributed_Transaction_Prepare", maxFetchEnd, tr.adtCommitBegin));
            lst.add(genTraceRecord("Distributed_Transaction_Commit", tr.adtCommitBegin, tr.adtCommitEnd));
        }
        lst.add(genTraceRecord("Write_to_Client", minFetchStart, tr.requestEnd));
        return false;
    }

    // slow log
    public List<String[]> genLogResult() {
        if (!tr.isDetailTrace) return null;
        List<String[]> lst = new ArrayList<>();
        if (tr.isCompletedV2()) {
            lst.add(genLogRecord("Read_SQL", tr.requestStart, tr.parseStart));
            lst.add(genLogRecord("Prepare_Push", tr.parseStart, tr.preExecuteEnd));
            if (tr.simpleHandler != null) {
                if (genSimpleLogs(lst)) return null;
            } else if (tr.builder != null) {
                if (genComplexQueryLogs(lst)) return null;
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("not support trace this query");
                }
                return null;
            }
        } else {
            if (tr.isCompletedV1() && tr.isNonBusinessSql()) {
                lst.add(genLogRecord("Read_SQL", tr.requestStart, tr.parseStart));
                lst.add(genLogRecord("Inner_Execute", tr.parseStart, tr.requestEnd));
                lst.add(genLogRecord("Write_Client", tr.requestEnd, tr.requestEnd));
            } else {
                if (LOGGER.isDebugEnabled()) {
                    long requestCount = tr.backendRouteList.size();
                    long firstRevCount = tr.backendRouteList.stream().filter(f -> f.getFirstRevTime() != 0).count();
                    long finishedCount = tr.backendRouteList.stream().filter(f -> f.getFinished() != 0).count();
                    long recordStartCount = tr.complexHandlerList.size();
                    long recordEndCount = tr.complexHandlerList.stream().filter(f -> f.endTime != 0).count();
                    LOGGER.debug("collect info not in pairs; requestEnd:{}, requestCount:{}, firstRevCount:{}, finishedCount:{}, recordStartCount:{}, recordEndCount:{}",
                            tr.requestEnd, requestCount, firstRevCount, finishedCount, recordStartCount, recordEndCount);
                }
                return null;
            }
        }
        return lst;
    }

    private boolean genComplexQueryLogs(List<String[]> lst) {
        List<ReferenceHandlerInfo> results = ComplexQueryPlanUtil.getComplexQueryResult(tr.builder);
        long lastChildFinished = tr.preExecuteEnd;
        for (ReferenceHandlerInfo result : results) {
            DMLResponseHandler handler = result.getHandler();
            if (handler instanceof BaseSelectHandler) {
                List<TraceResult.BackendRoute> bRs = tr.findByHandler(handler);
                if (LOGGER.isDebugEnabled()) {
                    long firstRevCount = bRs.stream().filter(f -> f.getFirstRevTime() != 0).count();
                    long finishedCount = bRs.stream().filter(f -> f.getFinished() != 0).count();
                    if (CollectionUtil.isEmpty(bRs) || firstRevCount != 1 || finishedCount != 1) {
                        LOGGER.debug("collect info not in pairs for connection; firstRevCount is {}, finishedCount is {}", firstRevCount, finishedCount);
                    }
                    return true;
                }
                TraceResult.BackendRoute ar = bRs.iterator().next();
                if (!result.isNestLoopQuery()) {
                    lst.add(genLogRecord(result.getName() + "_First_Result_Fetch", lastChildFinished, ar.getFirstRevTime()));
                } else {
                    TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                    if (complexHandler == null || complexHandler.getEndTime() == 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("collect info not in pairs for handler" + handler);
                        }
                        return true;
                    }
                    lst.add(genLogRecord("Generate_New_Query", lastChildFinished, complexHandler.getStartTime()));
                    lst.add(genLogRecord(result.getName() + "_First_Result_Fetch", complexHandler.getStartTime(), complexHandler.getEndTime()));
                }
                lst.add(genLogRecord(result.getName() + "_Last_Result_Fetch", ar.getFirstRevTime(), ar.getFinished()));
            } else if (handler instanceof OutputHandler) {
                TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                if (complexHandler == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for OutputHandler");
                    }
                    return true;
                }
                lst.add(genLogRecord("Write_Client", complexHandler.getStartTime(), tr.requestEnd));
            } else {
                TraceResult.ComplexHandler complexHandler = tr.findByComplexHandler(handler);
                if (complexHandler == null || complexHandler.getEndTime() == 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("collect info not in pairs for handler" + handler);
                    }
                    return true;
                }
                if (handler.getNextHandler() == null) {
                    lastChildFinished = Math.max(lastChildFinished, complexHandler.getEndTime());
                }
            }
        }
        return false;
    }

    private boolean genSimpleLogs(List<String[]> lst) {
        List<TraceResult.BackendRoute> bRs = tr.findByHandler(tr.simpleHandler);
        List<String[]> executeList = new ArrayList<>(bRs.size());
        List<String[]> fetchList = new ArrayList<>(bRs.size());
        long minFetchStart = Long.MAX_VALUE;
        long maxFetchEnd = 0;
        for (TraceResult.BackendRoute ar : bRs) {
            minFetchStart = Math.min(minFetchStart, ar.getFirstRevTime());
            executeList.add(genLogRecord(ar.getShardingNode() + "_First_Result_Fetch", tr.preExecuteEnd, ar.getFirstRevTime()));
            if (ar.getFinished() == 0) {
                LOGGER.debug("connection fetchEndRecord is null ");
                return true;
            }
            fetchList.add(genLogRecord(ar.getShardingNode() + "_Last_Result_Fetch", ar.getFirstRevTime(), ar.getFinished()));
            maxFetchEnd = Math.max(maxFetchEnd, ar.getFinished());
        }
        lst.addAll(executeList);
        lst.addAll(fetchList);
        lst.add(genLogRecord("Write_Client", minFetchStart, tr.requestEnd));
        return false;
    }

    // calculate
    private boolean genTraceRecord(List<String[]> lst, String operation, long start0, long end0) {
        return genTraceRecord(lst, operation, start0, end0, 0);
    }

    private boolean genTraceRecord(List<String[]> lst, String operation, long start0, long end0, long finalEnd) {
        if (end0 == 0) {
            if (finalEnd != 0) {
                lst.add(genTraceRecord(operation, start0, finalEnd));
                lst.add(genTraceRecord("Write_to_Client", finalEnd, finalEnd));
            } else {
                lst.add(genTraceRecord(operation, start0));
            }
            return true;
        } else {
            lst.add(genTraceRecord(operation, start0, end0));
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
        readQuery[1] = nanoToMilliSecond(start - tr.requestStart);
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
        readQuery[1] = nanoToMilliSecond(start - tr.requestStart);
        readQuery[2] = nanoToMilliSecond(end - tr.requestStart);
        readQuery[3] = nanoToMilliSecond(end - start);
        readQuery[4] = shardingNode;
        readQuery[5] = ref.replaceAll("[\\t\\n\\r]", " ");
        return readQuery;
    }

    private String[] genLogRecord(String operation, long start, long end) {
        String[] readQuery = new String[2];
        readQuery[0] = operation;
        readQuery[1] = nanoToSecond(end - start);
        return readQuery;
    }

    private String nanoToMilliSecond(long nano) {
        double milliSecond = (double) nano / 1000000;
        return String.valueOf(milliSecond);
    }

    private String nanoToSecond(long nano) {
        double milliSecond = (double) nano / 1000000000;
        return String.format("%.6f", milliSecond);
    }

}
