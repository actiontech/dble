/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.*;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.HandlerBuilder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.BaseDDLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.MultiNodeDdlPrepareHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionHandlerManager;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.oceanbase.obsharding_d.backend.mysql.store.memalloc.MemSizeController;
import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.btrace.provider.ComplexQueryProvider;
import com.oceanbase.obsharding_d.btrace.provider.CostTimeProvider;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.meta.DDLProxyMetaManager;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.handler.BackEndDataCleaner;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.optimizer.MyOptimizer;
import com.oceanbase.obsharding_d.plan.optimizer.SelectedProcessor;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.plan.visitor.MySQLPlanNodeVisitor;
import com.oceanbase.obsharding_d.plan.visitor.UpdatePlanNodeVisitor;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidUpdateParser;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.status.LoadDataBatch;
import com.oceanbase.obsharding_d.server.status.SlowQueryLog;
import com.oceanbase.obsharding_d.server.trace.TraceResult;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.DDLTraceHelper;
import com.oceanbase.obsharding_d.singleton.PauseShardingNodeManager;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.sql.StatisticListener;
import com.oceanbase.obsharding_d.statistic.stat.QueryTimeCost;
import com.oceanbase.obsharding_d.statistic.stat.QueryTimeCostContainer;
import com.oceanbase.obsharding_d.util.exception.NeedDelayedException;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static com.oceanbase.obsharding_d.meta.PauseEndThreadPool.CONTINUE_TYPE_MULTIPLE;
import static com.oceanbase.obsharding_d.meta.PauseEndThreadPool.CONTINUE_TYPE_SINGLE;

/**
 * @author mycat
 */
public class NonBlockingSession extends Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingSession.class);

    private long queryStartTime = 0;
    private final ShardingService shardingService;
    private final ConcurrentMap<RouteResultsetNode, BackendConnection> target;

    private SavePointHandler savePointHandler;
    private final TransactionHandlerManager transactionManager;


    private volatile boolean needWaitFinished = false;

    // kill query
    private volatile boolean killed = false;
    private volatile boolean discard = false;

    private OutputHandler outputHandler;

    // the memory controller for join,orderby,other in this session
    private final MemSizeController joinBufferMC;
    private final MemSizeController orderBufferMC;
    private final MemSizeController otherBufferMC;
    private QueryTimeCost queryTimeCost;
    private CostTimeProvider provider;
    private ComplexQueryProvider xprovider;
    private volatile boolean timeCost = false;
    private final AtomicBoolean firstBackConRes = new AtomicBoolean(false);

    private volatile boolean traceEnable = false;
    private final TraceResult traceResult = new TraceResult();
    private volatile RouteResultset complexRrs = null;
    private volatile SessionStage sessionStage = SessionStage.Init;

    private volatile long rowCountCurrentSQL = -1;
    private volatile long rowCountLastSQL = 0;
    private volatile boolean isoCharset;

    private final HashSet<BackendConnection> flowControlledTarget = new HashSet<>();

    public NonBlockingSession(ShardingService service) {
        this.shardingService = service;
        this.target = new ConcurrentHashMap<>(2, 1f);
        this.joinBufferMC = new MemSizeController(1024L * 1024L * SystemConfig.getInstance().getJoinMemSize());
        this.orderBufferMC = new MemSizeController(1024L * 1024L * SystemConfig.getInstance().getOrderMemSize());
        this.otherBufferMC = new MemSizeController(1024L * 1024L * SystemConfig.getInstance().getOtherMemSize());
        this.transactionManager = new TransactionHandlerManager(this);
        if (SystemConfig.getInstance().getUseSerializableMode() == 1) {
            transactionManager.setXaTxEnabled(true, service);
        }
    }

    public void setOutputHandler(OutputHandler outputHandler) {
        this.outputHandler = outputHandler;
    }

    private void sqlTracking(Consumer<TraceResult> consumer) {
        try {
            if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
                Optional.ofNullable(traceResult).ifPresent(consumer);
            }
        } catch (Exception e) {
            // Should not affect the main task
            LOGGER.warn("sqlTracking occurred: {}", e);
        }
    }

    public void setRequestTime() {
        sessionStage = SessionStage.Read_SQL;
        sqlTracking(t -> t.setRequestTime());

        if (SystemConfig.getInstance().getUseCostTimeStat() == 0) {
            return;
        }
        timeCost = false;
        if (ThreadLocalRandom.current().nextInt(100) >= SystemConfig.getInstance().getCostSamplePercent()) {
            return;
        }
        timeCost = true;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear");
        }
        queryTimeCost = new QueryTimeCost();
        provider = new CostTimeProvider();
        xprovider = new ComplexQueryProvider();
        provider.beginRequest(shardingService.getConnection().getId());

        long requestTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frontend connection setRequestTime:" + requestTime);
        }
        queryTimeCost.setRequestTime(requestTime);
    }

    public void startProcess() {
        sessionStage = SessionStage.Parse_SQL;
        sqlTracking(t -> t.startProcess());

        if (!timeCost) {
            return;
        }
        provider.startProcess(shardingService.getConnection().getId());
    }

    public void endParse() {
        sessionStage = SessionStage.Route_Calculation;
        sqlTracking(t -> t.endParse());

        if (!timeCost) {
            return;
        }
        provider.endParse(shardingService.getConnection().getId());
    }

    public void endRoute(RouteResultset rrs) {
        sessionStage = SessionStage.Prepare_to_Push;
        sqlTracking(t -> t.endRoute());

        if (!timeCost) {
            return;
        }
        provider.endRoute(shardingService.getConnection().getId());
        queryTimeCost.setCount(rrs.getNodes() == null ? 0 : rrs.getNodes().length);
    }

    public void endComplexRoute() {
        if (!timeCost) {
            return;
        }
        xprovider.endRoute(shardingService.getConnection().getId());
    }

    public void endComplexExecute() {
        if (!timeCost) {
            return;
        }
        xprovider.endComplexExecute(shardingService.getConnection().getId());
    }

    public void readyToDeliver() {
        if (!timeCost) {
            return;
        }
        provider.readyToDeliver(shardingService.getConnection().getId());
    }

    public void setPreExecuteEnd(TraceResult.SqlTraceType type) {
        sessionStage = SessionStage.Execute_SQL;
        sqlTracking(t -> t.setPreExecuteEnd(type));
    }

    public long getRowCount() {
        return rowCountLastSQL;
    }

    public void setSubQuery() {
        sqlTracking(t -> t.setSubQuery());
    }

    public void setBackendRequestTime(MySQLResponseService service) {
        StatisticListener.getInstance().record(this, r -> r.onBackendSqlStart(service));
        if (!timeCost) {
            return;
        }
        long backendID = service.getConnection().getId();
        QueryTimeCost backendCost = new QueryTimeCost();
        long requestTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("backend connection[" + backendID + "] setRequestTime:" + requestTime);
        }
        backendCost.setRequestTime(requestTime);
        queryTimeCost.getBackEndTimeCosts().put(backendID, backendCost);
    }

    public void setBackendResponseTime(MySQLResponseService service) {
        sessionStage = SessionStage.Fetching_Result;
        // Optional.ofNullable(StatisticListener2.getInstance().getRecorder(this, r ->r.onBackendSqlFirstEnd(service));
        long responseTime = System.nanoTime();
        sqlTracking(t -> t.setBackendResponseTime(service, responseTime));

        if (!timeCost) {
            return;
        }
        QueryTimeCost backCost = queryTimeCost.getBackEndTimeCosts().get(service.getConnection().getId());
        if (backCost != null && backCost.getResponseTime().compareAndSet(0, responseTime)) {
            if (queryTimeCost.getFirstBackConRes().compareAndSet(false, true)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("backend connection[" + service.getConnection().getId() + "] setResponseTime:" + responseTime);
                }
                provider.resFromBack(this.shardingService.getConnection().getId());
                firstBackConRes.set(false);
            }
            long index = queryTimeCost.getBackendReserveCount().decrementAndGet();
            if (index >= 0 && ((index % 10 == 0) || index < 10)) {
                provider.resLastBack(this.shardingService.getConnection().getId(), queryTimeCost.getBackendSize() - index);
            }
        }
    }

    public void startExecuteBackend() {
        if (!timeCost) {
            return;
        }
        if (firstBackConRes.compareAndSet(false, true)) {
            provider.startExecuteBackend(shardingService.getConnection().getId());
        }
        long index = queryTimeCost.getBackendExecuteCount().decrementAndGet();
        if (index >= 0 && ((index % 10 == 0) || index < 10)) {
            provider.execLastBack(shardingService.getConnection().getId(), queryTimeCost.getBackendSize() - index);
        }
    }

    public void allBackendConnReceive() {
        if (!timeCost) {
            return;
        }
        provider.allBackendConnReceive(shardingService.getConnection().getId());
    }

    public void setResponseTime(boolean isSuccess) {
        sessionStage = SessionStage.Finished;

        sqlTracking(t -> t.setResponseTime(shardingService, isSuccess));

        if (!timeCost) {
            return;
        }
        long responseTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("setResponseTime:" + responseTime);
        }
        queryTimeCost.getResponseTime().set(responseTime);
        provider.beginResponse(shardingService.getConnection().getId());
        QueryTimeCostContainer.getInstance().add(queryTimeCost);
    }

    public void setBackendResponseEndTime(MySQLResponseService service) {
        sessionStage = SessionStage.First_Node_Fetched_Result;
        StatisticListener.getInstance().record(this, r -> r.onBackendSqlEnd(service));
        sqlTracking(t -> t.setBackendResponseEndTime(service));

        if (!timeCost) {
            return;
        }
        if (queryTimeCost.getFirstBackConEof().compareAndSet(false, true)) {
            xprovider.firstComplexEof(this.shardingService.getConnection().getId());
        }
    }

    public void setBeginCommitTime() {
        sessionStage = SessionStage.Distributed_Transaction_Commit;
        sqlTracking(t -> t.setAdtCommitBegin());
    }

    public void setFinishedCommitTime() {
        sqlTracking(t -> t.setAdtCommitEnd());
    }

    public void setHandlerStart(DMLResponseHandler handler) {
        sqlTracking(t -> t.addToRecordStartMap(handler));
    }

    public void setHandlerEnd(DMLResponseHandler handler) {
        if (handler.getNextHandler() != null) {
            DMLResponseHandler next = handler.getNextHandler();
            sessionStage = SessionStage.changeFromHandlerType(next.type());
        }
        sqlTracking(t -> t.addToRecordEndMap(handler));
    }

    public List<String[]> genTraceResult() {
        if (traceEnable) {
            return traceResult.genShowTraceResult();
        } else {
            return null;
        }
    }

    public List<String[]> genRunningSQLStage() {
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            TraceResult tmpResult = traceResult.clone();
            return tmpResult.genRunningSQLStage();
        } else {
            return null;
        }
    }

    public FrontendConnection getSource() {
        return (FrontendConnection) shardingService.getConnection();
    }

    public int getTargetCount() {
        return target.size();
    }

    public Set<RouteResultsetNode> getTargetKeys() {
        return target.keySet();
    }

    public BackendConnection getTarget(RouteResultsetNode key) {
        return target.get(key);
    }

    public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
        return this.target;
    }

    public boolean isNeedWaitFinished() {
        return needWaitFinished;
    }

    public SessionStage getSessionStage() {
        return sessionStage;
    }

    public void execute(RouteResultset rrs) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(shardingService, "execute-sql-for-sharding");
        TraceManager.log(ImmutableMap.of("route-result-set", rrs), traceObject);
        try {
            if (killed) {
                LOGGER.info("{} sql[{}] is killed.", getShardingService().toString2(), getShardingService().getExecuteSql());
                shardingService.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
                return;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} print current {}", shardingService.toString2(), rrs);
            }

            if (PauseShardingNodeManager.getInstance().getIsPausing().get() &&
                    !PauseShardingNodeManager.getInstance().checkTarget(target) &&
                    PauseShardingNodeManager.getInstance().checkRRS(rrs)) {
                if (PauseShardingNodeManager.getInstance().waitForResume(rrs, shardingService, CONTINUE_TYPE_SINGLE)) {
                    return;
                }
            }

            // complex query
            RouteResultsetNode[] nodes = rrs.getNodes();
            if (nodes == null || nodes.length == 0 || nodes[0].getName() == null || nodes[0].getName().equals("")) {
                if (rrs.isNeedOptimizer()) {
                    try {
                        if (rrs.getSqlStatement() instanceof SQLSelectStatement) {
                            this.complexRrs = rrs;
                            executeMultiSelect(rrs);
                        } else if (rrs.getSqlStatement() instanceof SQLUpdateStatement) {
                            this.complexRrs = rrs;
                            executeMultiUpdate(rrs);
                        }
                    } catch (MySQLOutPutException e) {
                        LOGGER.warn("execute complex sql cause error", e);
                        shardingService.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
                    }
                } else {
                    shardingService.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                            "No shardingNode found ,please check tables defined in schema:" + shardingService.getSchema());
                }
                return;
            }
            setRouteResultToTrace(nodes);
            if (rrs.getImplicitlyCommitHandler() != null) {
                executeImplicitlyCommitSql(rrs);
            } else {
                // dml or simple select
                executeOther(rrs);
            }
        } finally {
            TraceManager.finishSpan(shardingService, traceObject);
        }
    }

    public void setRouteResultToTrace(RouteResultsetNode[] nodes) {
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setShardingNodes(nodes);
        }
    }

    private void executeImplicitlyCommitSql(RouteResultset rrs) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(shardingService, "execute-sql-for-ddl");
        ExecutableHandler executableHandler = null;
        try {
            if (null != rrs.getImplicitlyCommitHandler()) {
                if (rrs.getImplicitlyCommitHandler() instanceof BaseDDLHandler) {
                    DDLTraceHelper.init(shardingService, rrs.getSrcStatement());
                }

                addMetaLock(rrs);

                if (rrs.getNodes().length == 1) {
                    executableHandler = rrs.getImplicitlyCommitHandler();
                    setPreExecuteEnd(TraceResult.SqlTraceType.SINGLE_NODE_QUERY);
                } else {
                    /*
                     * here, just a try! The sync is the superfluous, because there are heartbeats  at every backend node.
                     * We don't do 2pc or 3pc. Because mysql(that is, resource manager) don't support that for ddl statements.
                     */
                    executableHandler = rrs.getImplicitlyCommitHandler();
                    if (executableHandler instanceof MultiNodeDdlPrepareHandler) {
                        checkBackupStatus();
                    }
                    setPreExecuteEnd(TraceResult.SqlTraceType.MULTI_NODE_QUERY);
                }

                setTraceSimpleHandler((ResponseHandler) executableHandler);
                readyToDeliver();
                ClusterDelayProvider.delayDdLToDeliver();
                executableHandler.execute();
                discard = true;
            } else {
                throw new Exception("no processor to perform!");
            }
        } catch (Exception e) {
            LOGGER.info(String.valueOf(shardingService) + rrs, e);
            if (null != executableHandler) {
                if (executableHandler instanceof BaseDDLHandler) {
                    ((BaseDDLHandler) executableHandler).executeFail(e.getMessage());
                    return;
                } else {
                    executableHandler.clearAfterFailExecute();
                }
            }
            DDLTraceHelper.finish(shardingService);
            shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
        } finally {
            TraceManager.finishSpan(shardingService, traceObject);
        }
    }

    private void executeOther(RouteResultset rrs) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(shardingService, "execute-for-dml");
        ExecutableHandler executableHandler = null;
        try {
            if (rrs.getNodes().length == 1 && !rrs.isEnableLoadDataFlag()) {
                executableHandler = new SingleNodeHandler(rrs, this);
                setPreExecuteEnd(TraceResult.SqlTraceType.SINGLE_NODE_QUERY);
            } else if (ServerParse.SELECT == rrs.getSqlType() && rrs.getGroupByCols() != null) {
                executableHandler = new MultiNodeSelectHandler(rrs, this);
                setPreExecuteEnd(TraceResult.SqlTraceType.MULTI_NODE_GROUP);
            } else if (ServerParse.LOAD_DATA_INFILE_SQL == rrs.getSqlType() && LoadDataBatch.getInstance().isEnableBatchLoadData()) {
                executableHandler = new MultiNodeLoadDataHandler(rrs, this);
                setPreExecuteEnd(TraceResult.SqlTraceType.MULTI_NODE_GROUP);
            } else {
                executableHandler = new MultiNodeQueryHandler(rrs, this);
                setPreExecuteEnd(TraceResult.SqlTraceType.MULTI_NODE_QUERY);
            }

            setTraceSimpleHandler((ResponseHandler) executableHandler);

            readyToDeliver();

            try {
                executableHandler.execute();
                discard = true;
            } catch (Exception e) {
                LOGGER.info(String.valueOf(shardingService) + rrs, e);
                executableHandler.writeRemainBuffer();
                executableHandler.clearAfterFailExecute();
                shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
        } finally {
            if (executableHandler != null) {
                TraceManager.log(ImmutableMap.of("executableHandler", executableHandler), traceObject);
            }
            TraceManager.finishSpan(traceObject);
        }
    }

    public void setTraceBuilder(BaseHandlerBuilder baseBuilder) {
        sqlTracking(t -> t.setBuilder(baseBuilder));
    }

    private void executeMultiResultSet(RouteResultset rrs, PlanNode node) {
        init();
        HandlerBuilder builder = new HandlerBuilder(node, this);
        try {
            RouteResultsetNode rrsNode = builder.build(rrs.isHaveHintPlan2Inner());
            if (rrsNode != null) {
                RouteResultsetNode[] nodes = {rrsNode};
                rrs.setNodes(nodes);
                setRouteResultToTrace(nodes);
                // dml or simple select
                executeOther(rrs);
            }
            discard = true;
        } catch (SQLSyntaxErrorException e) {
            LOGGER.info(shardingService + " execute plan is : " + node, e);
            shardingService.writeErrMessage(ErrorCode.ER_YES, "optimizer build error");
        } catch (NoSuchElementException e) {
            LOGGER.info(shardingService + " execute plan is : " + node, e);
            this.closeAndClearResources("Exception");
            shardingService.writeErrMessage(ErrorCode.ER_NO_VALID_CONNECTION, "no valid connection");
        } catch (MySQLOutPutException e) {
            LOGGER.info(shardingService + " execute plan is : " + node, e);
            this.closeAndClearResources("Exception");
            shardingService.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            LOGGER.info(shardingService + " execute plan is : " + node, e);
            this.closeAndClearResources("Exception");
            shardingService.writeErrMessage(ErrorCode.ER_HANDLE_DATA, e.toString());
        }
    }

    public void executeMultiSelect(RouteResultset rrs) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(shardingService, "try-complex-query");
        try {
            SQLSelectStatement ast = (SQLSelectStatement) rrs.getSqlStatement();
            MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, shardingService.getUsrVariables(), rrs.getHintPlanInfo());
            visitor.visit(ast);
            PlanNode node = visitor.getTableNode();
            if (node.isCorrelatedSubQuery()) {
                throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Correlated Sub Queries is not supported ");
            }
            node.setSql(rrs.getStatement());
            node.setUpFields();
            PlanUtil.checkTablesPrivilege(shardingService, node, ast);
            node = MyOptimizer.optimize(node, rrs.getHintPlanInfo());

            if (PauseShardingNodeManager.getInstance().getIsPausing().get() &&
                    !PauseShardingNodeManager.getInstance().checkTarget(target) &&
                    PauseShardingNodeManager.getInstance().checkReferredTableNodes(node.getReferedTableNodes())) {
                if (PauseShardingNodeManager.getInstance().waitForResume(rrs, this.shardingService, CONTINUE_TYPE_MULTIPLE)) {
                    return;
                }
            }
            setPreExecuteEnd(TraceResult.SqlTraceType.COMPLEX_QUERY);
            if (PlanUtil.containsSubQuery(node)) {
                setSubQuery();
                final PlanNode finalNode = node;
                //sub Query build will be blocked, so use ComplexQueryExecutor
                OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                    executeMultiResultSet(rrs, finalNode);
                });
            } else {
                if (!visitor.isContainSchema()) {
                    node.setAst(ast);
                }
                executeMultiResultSet(rrs, node);
            }
        } finally {
            TraceManager.finishSpan(shardingService, traceObject);
        }
    }

    public void executeMultiUpdate(RouteResultset rrs) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(shardingService, "try-complex-update");
        try {
            MySqlUpdateStatement ast = (MySqlUpdateStatement) rrs.getSqlStatement();
            UpdatePlanNodeVisitor visitor = new UpdatePlanNodeVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, shardingService.getUsrVariables(), rrs.getHintPlanInfo());
            visitor.visit(ast);
            PlanNode node = visitor.getTableNode();
            if (node.isCorrelatedSubQuery()) {
                throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Correlated Sub Queries is not supported ");
            }
            node.setSql(rrs.getStatement());
            node.setUpFields();
            PlanUtil.checkTablesPrivilege(shardingService, node, ast);
            //sub query
            node = SelectedProcessor.optimize(node);

            if (PauseShardingNodeManager.getInstance().getIsPausing().get() &&
                    !PauseShardingNodeManager.getInstance().checkTarget(target) &&
                    PauseShardingNodeManager.getInstance().checkReferredTableNodes(node.getReferedTableNodes())) {
                if (PauseShardingNodeManager.getInstance().waitForResume(rrs, this.shardingService, CONTINUE_TYPE_MULTIPLE)) {
                    return;
                }
            }
            setPreExecuteEnd(TraceResult.SqlTraceType.COMPLEX_MODIFY);
            if (PlanUtil.containsSubQuery(node)) {
                setSubQuery();
                final PlanNode finalNode = node;
                //sub Query build will be blocked, so use ComplexQueryExecutor
                OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                    executeMultiResultSet(rrs, finalNode);
                });
            } else {
                throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", DruidUpdateParser.MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        } finally {
            TraceManager.finishSpan(shardingService, traceObject);
        }
    }

    private void addMetaLock(RouteResultset rrs) throws SQLNonTransientException {
        // filtering: hint ddl、online ddl 、create/drop/alter/replace view、create database、create table、lock table/
        if (rrs.getSchema() == null || rrs.isOnline() ||
                rrs.getSqlType() == ServerParse.CREATE_VIEW ||
                rrs.getSqlType() == ServerParse.DROP_VIEW ||
                rrs.getSqlType() == ServerParse.ALTER_VIEW ||
                rrs.getSqlType() == ServerParse.REPLACE_VIEW ||
                rrs.getSqlType() == ServerParse.CREATE_DATABASE ||
                rrs.getSqlType() == ServerParse.LOCK) {
            return;
        }

        String schema = rrs.getSchema();
        String table = rrs.getTable();
        try {
            DDLProxyMetaManager.Originator.notifyClusterDDLPrepare(shardingService, schema, table, rrs.getStatement());
            //lock self meta
            DDLProxyMetaManager.Originator.addTableMetaLock(shardingService, schema, table, rrs.getStatement());
        } catch (NeedDelayedException e) {
            // not happen
            throw e;
        } catch (Exception e) {
            throw new SQLNonTransientException(e.getMessage());
        }
    }

    private void init() {
        this.outputHandler = null;
    }

    public void onQueryError(byte[] message) {
        if (outputHandler != null) {
            outputHandler.backendConnError(message);
        } else {
            String error = new String(message);
            this.closeAndClearResources(error);
            shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, error);
        }
    }

    public TransactionHandlerManager getTransactionManager() {
        return transactionManager;
    }

    public void commit(TransactionCallback callback) {
        checkBackupStatus();
        transactionManager.commit(callback);
    }

    public void rollback(TransactionCallback callback) {
        checkBackupStatus();
        transactionManager.rollback(callback);
    }

    public void syncImplicitCommit() throws SQLException {
        if (shardingService.isInTransaction()) {
            if (shardingService.isTxInterrupted()) {
                throw new SQLException(shardingService.getTxInterruptMsg(), "HY000", ErrorCode.ER_YES);
            }
            checkBackupStatus();
            transactionManager.syncImplicitCommit();
        }
    }

    public void performSavePoint(String spName, SavePointHandler.Type type) {
        if (savePointHandler == null) {
            savePointHandler = new SavePointHandler(this);
        }
        savePointHandler.perform(spName, type);
    }

    public void clearSavepoint() {
        if (savePointHandler != null) {
            savePointHandler.clearResources();
        }
    }

    public void checkBackupStatus() {
        while (OBsharding_DServer.getInstance().isBackupLocked()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        needWaitFinished = true;
    }


    public void unLockTable(String sql) {
        UnLockTablesHandler handler = new UnLockTablesHandler(this, this.shardingService.isAutocommit(), sql);
        handler.execute();
    }


    /**
     * {@link } must be true before invoking this
     */
    public void terminate() {
        // XA MUST BE FINISHED
        if ((shardingService.isInTransaction() && transactionManager.getXAStage() != null) ||
                needWaitFinished) {
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("terminate {}", this);
        }
        for (BackendConnection node : target.values()) {
            node.close("client closed or timeout killed");
        }
        target.clear();
    }

    public void closeAndClearResources(String reason) {
        // XA MUST BE FINISHED
        if (shardingService.isInTransaction() && transactionManager.getXAStage() != null) {
            return;
        }
        for (BackendConnection node : target.values()) {
            node.businessClose(reason);
        }
        target.clear();
    }

    public void forceClose(String reason) {
        for (BackendConnection node : target.values()) {
            node.businessClose(reason);
        }
        target.clear();
    }

    public void releaseConnectionIfSafe(MySQLResponseService service, boolean needClosed) {
        RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
        if (node != null) {
            if ((this.shardingService.isAutocommit() || service.getConnection().isFromSlaveDB()) && !this.shardingService.isTxStart() && !this.shardingService.isLockTable()) {
                releaseConnection((RouteResultsetNode) service.getAttachment(), needClosed);
            }
        }
    }

    public void releaseConnection(RouteResultsetNode rrn, final boolean needClose) {
        if (rrn != null) {
            BackendConnection c = target.remove(rrn);
            if (c != null && !c.isClosed()) {
                if (shardingService.isFlowControlled()) {
                    releaseConnectionFromFlowControlled(c);
                }
                if (c.getService().isAutocommit()) {
                    c.release();
                } else if (needClose) {
                    //c.rollback();
                    c.close("the need to be closed");
                } else {
                    c.release();
                }
            }
        }
    }

    public void releaseConnection(BackendConnection con) {
        Iterator<Entry<RouteResultsetNode, BackendConnection>> iterator = target.entrySet().iterator();
        while (iterator.hasNext()) {
            BackendConnection theCon = iterator.next().getValue();
            if (theCon == con) {
                iterator.remove();
                con.release();
                break;
            }
        }
    }

    public void waitFinishConnection(RouteResultsetNode rrn) {
        BackendConnection c = target.get(rrn);
        if (c != null) {
            BackEndDataCleaner clear = new BackEndDataCleaner((MySQLResponseService) c.getService());
            clear.waitUntilDataFinish();
        }
    }

    // thread may not safe
    public void releaseConnections(final boolean needClosed) {
        for (RouteResultsetNode rrn : target.keySet()) {
            releaseConnection(rrn, needClosed);
        }
    }

    public void bindConnection(RouteResultsetNode key, BackendConnection conn) {
        conn.setBindFront(this.getSource().getSimple());
        target.put(key, conn);
    }

    public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("try-exists-connection");
        try {
            if (conn == null) {
                return false;
            }

            boolean canReUse = false;
            if (conn.isFromSlaveDB() && (node.canRunINReadDB(shardingService.isAutocommit()) &&
                    (node.getRunOnSlave() == null || node.getRunOnSlave()))) {
                canReUse = true;
            }

            if (!conn.isFromSlaveDB()) {
                canReUse = true;
            }

            if (canReUse) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("found connections in session to use " + conn + " for " + node);
                }
                ((MySQLResponseService) conn.getService()).setAttachment(node);
                return true;
            } else {
                // slave db connection and can't use anymore ,release it
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("release slave connection,can't be used in trasaction  " + conn + " for " + node);
                }
                releaseConnection(node, false);
            }
            return false;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void kill() {
        AtomicInteger count = new AtomicInteger(0);
        Map<RouteResultsetNode, BackendConnection> toKilled = new HashMap<>();

        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : target.entrySet()) {
            BackendConnection c = entry.getValue();
            if (c != null) {
                toKilled.put(entry.getKey(), c);
                count.incrementAndGet();
            }
        }

        for (Entry<RouteResultsetNode, BackendConnection> en : toKilled.entrySet()) {
            KillConnectionHandler kill = new KillConnectionHandler(en.getValue(), this);
            ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
            ShardingNode dn = conf.getShardingNodes().get(en.getKey().getName());
            try {
                dn.getConnectionFromSameSource(en.getValue().getSchema(), en.getValue(), kill, en.getKey());
            } catch (Exception e) {
                LOGGER.info("get killer connection failed for " + en.getKey(), e);
                kill.connectionError(e, null);
            }
        }
    }

    public void clearResources(final boolean needClosed) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear session resources " + this);
        }
        if (!shardingService.isLockTable()) {
            this.releaseConnections(needClosed);
        }
        if (!transactionManager.isRetryXa()) {
            transactionManager.setRetryXa(true);
        }
        needWaitFinished = false;
    }

    public boolean closed() {
        return shardingService.getConnection().isClosed();
    }

    public ShardingService getShardingService() {
        return shardingService;
    }

    public String getSessionXaID() {
        return transactionManager.getSessionXaID();
    }


    public MySQLResponseService freshConn(BackendConnection errConn, ResponseHandler queryHandler) {
        for (final RouteResultsetNode node : this.getTargetKeys()) {
            final BackendConnection mysqlCon = this.getTarget(node);
            if (errConn.equals(mysqlCon)) {
                ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
                ShardingNode dn = conf.getShardingNodes().get(node.getName());
                try {
                    BackendConnection newConn = dn.getConnection(dn.getDatabase(), false, errConn.getBackendService().getAttachment());
                    newConn.getBackendService().setXaStatus(errConn.getBackendService().getXaStatus());
                    newConn.getBackendService().setSession(this);
                    newConn.getBackendService().setResponseHandler(queryHandler);
                    if (newConn.isClosed()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("BackendConnection[{}] is closed.", newConn);
                        }
                        return errConn.getBackendService();
                    }
                    errConn.businessClose("error connection change in xa");
                    this.bindConnection(node, newConn);
                    return newConn.getBackendService();
                } catch (Exception e) {
                    return errConn.getBackendService();
                }
            }
        }
        return errConn.getBackendService();
    }


    public void rowCountRolling() {
        rowCountLastSQL = rowCountCurrentSQL;
        rowCountCurrentSQL = -1;
    }

    public void setRowCount(long rowCount) {
        this.rowCountCurrentSQL = rowCount;
    }

    public MemSizeController getJoinBufferMC() {
        return joinBufferMC;
    }

    public MemSizeController getOrderBufferMC() {
        return orderBufferMC;
    }

    public MemSizeController getOtherBufferMC() {
        return otherBufferMC;
    }

    public AtomicInteger getPacketId() {
        return shardingService.getPacketId();
    }

    public long getQueryStartTime() {
        return queryStartTime;
    }

    public void setQueryStartTime(long queryStartTime) {
        this.queryStartTime = queryStartTime;
    }


    public boolean isTrace() {
        return traceEnable;
    }

    public void setTrace(boolean enable) {
        traceEnable = enable;
    }

    public void setTraceSimpleHandler(ResponseHandler simpleHandler) {
        sqlTracking(t -> t.setSimpleHandler(simpleHandler));
    }

    public RouteResultset getComplexRrs() {
        return complexRrs;
    }

    public void setRetryXa(boolean retryXa) {
        transactionManager.setRetryXa(retryXa);
    }

    public boolean isRetryXa() {
        return transactionManager.isRetryXa();
    }

    public boolean isKilled() {
        return killed;
    }

    public void setKilled(boolean killed) {
        this.killed = killed;
    }

    public boolean isDiscard() {
        return discard;
    }

    public void setDiscard(boolean discard) {
        this.discard = discard;
    }

    public boolean isIsoCharset() {
        return isoCharset;
    }

    public void setIsoCharset(boolean isoCharset) {
        this.isoCharset = isoCharset;
    }

    @Override
    public void stopFlowControl(int currentWritingSize) {
        synchronized (flowControlledTarget) {
            if (flowControlledTarget.size() == 0) {
                return;
            }
            shardingService.getConnection().setFrontWriteFlowControlled(false);
            Iterator<BackendConnection> iterator = flowControlledTarget.iterator();
            while (iterator.hasNext()) {
                BackendConnection con = iterator.next();
                if (con.getService() instanceof MySQLResponseService) {
                    int size = ((MySQLResponseService) (con.getService())).getReadSize();
                    if (size <= con.getFlowLowLevel()) {
                        con.enableRead();
                        iterator.remove();
                    } else {
                        if (LOGGER.isDebugEnabled())
                            LOGGER.debug("This front connection want to remove flow control, but mysql conn [{}]'s size [{}] is not lower the FlowLowLevel", con.getThreadId(), size);
                    }
                } else {
                    con.enableRead();
                    iterator.remove();
                }
            }
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("This front connection remove flow control, currentWritingSize= {} and now still have {} backend connections in flow control state, the front conn info :{} ", currentWritingSize, flowControlledTarget.size(), this.getSource());
        }
    }

    @Override
    public void startFlowControl(int currentWritingSize) {
        synchronized (flowControlledTarget) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("This front connection begins flow control, currentWritingSize= {},conn info:{}", currentWritingSize, this.getSource());
            shardingService.getConnection().setFrontWriteFlowControlled(true);
            for (BackendConnection con : target.values()) {
                con.disableRead();
                flowControlledTarget.add(con);
            }
        }
    }

    @Override
    public void releaseConnectionFromFlowControlled(BackendConnection con) {
        synchronized (flowControlledTarget) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("This backend connection remove flow control because of release:{}", con);
            }
            if (flowControlledTarget.remove(con)) {
                con.getSocketWR().enableRead();
            }
            if (flowControlledTarget.size() == 0) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("This frontend connection remove flow control because of release:{} ", this.getSource());
                shardingService.getConnection().setFrontWriteFlowControlled(false);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NonBlockSession with target = [");
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : target.entrySet())
            sb.append(entry.getKey()).append(" with ").append(entry.getValue().toString2()).append(";");
        sb.append("]");
        return sb.toString();
    }

}
