/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.sqlvisitor.GlobalVisitor;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.*;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.groupby.AggregateHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.subquery.*;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.CallBackHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemSum;
import com.oceanbase.obsharding_d.plan.common.item.subquery.*;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode.PlanNodeType;
import com.oceanbase.obsharding_d.plan.node.QueryNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public abstract class BaseHandlerBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseHandlerBuilder.class);
    private static final AtomicLong SEQUENCE_ID = new AtomicLong(0);
    protected NonBlockingSession session;
    HandlerBuilder hBuilder;
    protected DMLResponseHandler start;
    /* the current last handler */
    private DMLResponseHandler currentLast;
    private final PlanNode node;
    Map<String, SchemaConfig> schemaConfigMap = new HashMap<>();
    /* the children can be push down */
    boolean canPushDown = false;
    /* need common handler? like group by,order by,limit and so on */
    boolean needCommon = true;
    /* has where handler */
    boolean needWhereHandler = true;

    protected boolean isExplain;
    private final List<BaseHandlerBuilder> subQueryBuilderList = new CopyOnWriteArrayList<>();
    protected boolean isFastBack;

    protected BaseHandlerBuilder(NonBlockingSession session, PlanNode node, HandlerBuilder hBuilder, boolean isExplain) {
        this.session = session;
        this.node = node;
        this.hBuilder = hBuilder;
        this.isExplain = isExplain;
        this.schemaConfigMap.putAll(OBsharding_DServer.getInstance().getConfig().getSchemas());
        if (schemaConfigMap.isEmpty())
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "current router config is empty!");
    }

    public DMLResponseHandler getEndHandler() {
        return currentLast;
    }

    public List<BaseHandlerBuilder> getSubQueryBuilderList() {
        return subQueryBuilderList;
    }

    /**
     * generate a handler chain
     */
    public final void build() {
        List<DMLResponseHandler> preHandlers = null;
        // is use the nest loop join
        boolean isNestLoopJoin = isNestLoopStrategy(node);
        if (isNestLoopJoin) {
            nestLoopBuild();
        } else if (PlanUtil.isGlobal(node)) {
            // the query can be send to a certain node
            noShardBuild();
        } else if (canDoAsMerge()) {
            // the query can be send to some certain nodes .eg: ER tables,  GLOBAL*NORMAL GLOBAL*ER
            RouteResultset routeResultset = tryMergeBuild();
            mergeBuild(routeResultset);
        } else {
            //need to split to simple query
            preHandlers = buildPre();
            if (isFastBack) {
                return;
            }
            buildOwn();
        }
        if (!node.isSingleRoute()) {
            if (needCommon) {
                buildCommon();
            }
            if (needCommon || node.isWithSubQuery() || node.haveDependOnNode()) {
                // view sub alias
                String tbAlias = node.getAlias();
                String schema = null;
                String table = null;
                if (node.type() == PlanNodeType.TABLE) {
                    TableNode tbNode = (TableNode) node;
                    schema = tbNode.getSchema();
                    table = tbNode.getTableName();
                }
                SendMakeHandler sh = new SendMakeHandler(getSequenceId(), session, node.getColumnsSelected(), schema, table, tbAlias);
                addHandler(sh);
                nestLoopAddHandler(sh);

            }
        }

        if (preHandlers != null) {
            for (DMLResponseHandler preHandler : preHandlers) {
                preHandler.setNextHandler(start);
            }
        }
    }


    void executeSubQueries(List<DMLResponseHandler> subQueryEndHandlers) {
        final ReentrantLock lock = new ReentrantLock();
        final Condition finishSubQuery = lock.newCondition();
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicInteger subNodes = new AtomicInteger(node.getSubQueries().size());
        final CopyOnWriteArrayList<ErrorPacket> errorPackets = new CopyOnWriteArrayList<>();

        for (DMLResponseHandler subQueryEndHandler : subQueryEndHandlers) {
            OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                boolean startHandler = false;
                try {
                    final SubQueryHandler tempHandler = (SubQueryHandler) (subQueryEndHandler.getNextHandler());
                    // clearExplain status
                    tempHandler.clearForExplain();
                    CallBackHandler tempDone = () -> {
                        if (tempHandler.getErrorPacket() != null) {
                            errorPackets.add(tempHandler.getErrorPacket());
                        }
                        subQueryFinished(subNodes, lock, finished, finishSubQuery);
                    };
                    tempHandler.setTempDoneCallBack(tempDone);
                    startHandler = true;
                    HandlerBuilder.startHandler(subQueryEndHandler);
                } catch (Exception e) {
                    LOGGER.info("execute SubQuery error", e);
                    ErrorPacket errorPackage = new ErrorPacket();
                    errorPackage.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
                    String errorMsg = e.getMessage() == null ? e.toString() : e.getMessage();
                    errorPackage.setMessage(errorMsg.getBytes(StandardCharsets.UTF_8));
                    errorPackets.add(errorPackage);
                    if (!startHandler) {
                        subQueryFinished(subNodes, lock, finished, finishSubQuery);
                    }
                }
            });
        }
        lock.lock();
        try {
            while (!finished.get()) {
                finishSubQuery.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("execute SubQuery error", e);
            ErrorPacket errorPackage = new ErrorPacket();
            errorPackage.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
            String errorMsg = e.getMessage() == null ? e.toString() : e.getMessage();
            errorPackage.setMessage(errorMsg.getBytes(StandardCharsets.UTF_8));
            errorPackets.add(errorPackage);
        } finally {
            lock.unlock();
        }
        if (errorPackets.size() > 0) {
            throw new MySQLOutPutException(errorPackets.get(0).getErrNo(), "", new String(errorPackets.get(0).getMessage(), StandardCharsets.UTF_8));
        }
    }

    protected void nestLoopBuild() {
        throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "not implement yet, node type[" + node.type() + "]");
    }

    protected abstract boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) throws SQLException;

    /* er join and  global *normal and table node are need not split to simple query*/
    protected boolean canDoAsMerge() {
        return false;
    }

    protected void mergeBuild(RouteResultset rrs) {
        //
    }

    protected RouteResultset tryMergeBuild() {
        //
        return null;
    }

    protected abstract List<DMLResponseHandler> buildPre();

    /**
     * build own handler
     */
    protected abstract void buildOwn();

    /**
     * no shard-ing node,just send to the first node
     */
    protected void noShardBuild() {
        this.needCommon = false;
        GlobalVisitor visitor = new GlobalVisitor(node, true, false);
        visitor.visit();
        String sql = visitor.getSql().toString();
        Map<String, String> mapTableToSimple = visitor.getMapTableToSimple();
        for (Map.Entry<String, String> tableToSimple : mapTableToSimple.entrySet()) {
            sql = sql.replace(tableToSimple.getKey(), tableToSimple.getValue());
        }
        String randomShardingNode = RouterUtil.getRandomShardingNode(node.getNoshardNode());
        Set<String> tableSet = mapTableToSimple.keySet().stream().map(s -> s.replace("`", "")).collect(Collectors.toSet());
        RouteResultsetNode rrsNode = new RouteResultsetNode(randomShardingNode, ServerParse.SELECT, sql, tableSet);
        RouteResultsetNode[] rrss = new RouteResultsetNode[]{rrsNode};
        hBuilder.checkRRSs(rrss);
        if (session.getTargetCount() > 0 && session.getTarget(rrss[0]) == null) {
            for (String shardingNode : node.getNoshardNode()) {
                if (!shardingNode.equals(randomShardingNode)) {
                    RouteResultsetNode tmpRrsNode = new RouteResultsetNode(shardingNode, ServerParse.SELECT, sql);
                    RouteResultsetNode[] tmpRrss = new RouteResultsetNode[]{tmpRrsNode};
                    hBuilder.checkRRSs(tmpRrss);
                    if (session.getTarget(tmpRrsNode) != null) {
                        rrss = tmpRrss;
                        hBuilder.removeRrs(rrsNode);
                        break;
                    } else {
                        hBuilder.removeRrs(tmpRrsNode);
                    }
                }
            }
        }

        MultiNodeMergeHandler mh = new MultiNodeEasyMergeHandler(getSequenceId(), rrss,
                !session.getShardingService().isInTransaction(),
                session, node.getNoshardNode());
        addHandler(mh);
    }

    /**
     * build common properties,like where,groupby,having,orderby,limit, and sendMakHandler(rename)
     */
    private void buildCommon() {
        if (node.getWhereFilter() != null && needWhereHandler) {
            WhereHandler wh = new WhereHandler(getSequenceId(), session, node.getWhereFilter());
            addHandler(wh);
        }
        /* need groupby handler */
        if (nodeHasGroupBy(node)) {
            boolean needOrderBy = (node.getGroupBys().size() > 0) && isOrderNeeded(node, node.getGroupBys());
            boolean canDirectGroupBy = true;
            List<ItemSum> sumRefs = new ArrayList<>();
            for (ItemSum funRef : node.getSumFuncs()) {
                if (funRef.hasWithDistinct() || funRef.sumType().equals(ItemSum.SumFuncType.GROUP_CONCAT_FUNC))
                    canDirectGroupBy = false;
                sumRefs.add(funRef);
            }
            if (needOrderBy) {
                if (canDirectGroupBy) {
                    // we go direct groupby
                    DirectGroupByHandler gh = new DirectGroupByHandler(getSequenceId(), session, node.getGroupBys(),
                            sumRefs);
                    addHandler(gh);
                } else {
                    OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getGroupBys());
                    addHandler(oh);
                    AggregateHandler gh = new AggregateHandler(getSequenceId(), session, node.getGroupBys(),
                            sumRefs);
                    addHandler(gh);
                }
            } else { // @bug 1052 canDirectGroupby condition we use
                // directgroupby already
                AggregateHandler gh = new AggregateHandler(getSequenceId(), session, node.getGroupBys(),
                        sumRefs);
                addHandler(gh);
            }
        }
        // having
        if (node.getHavingFilter() != null) {
            HavingHandler hh = new HavingHandler(getSequenceId(), session, node.getHavingFilter());
            addHandler(hh);
        }

        if (node.isDistinct() && node.getOrderBys().size() > 0) {
            // distinct and order by both exists
            List<Order> mergedOrders = mergeOrderBy(node.getColumnsSelected(), node.getOrderBys());
            if (mergedOrders == null) {
                // can not merge,need distinct then order by
                DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected());
                addHandler(dh);
                OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getOrderBys());
                addHandler(oh);
            } else {
                DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected(),
                        mergedOrders);
                addHandler(dh);
            }
        } else {
            if (node.isDistinct()) {
                DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected());
                addHandler(dh);
            }
            // order by
            if (node.getOrderBys().size() > 0) {
                if (node.getGroupBys().size() > 0) {
                    if (!PlanUtil.orderContains(node.getGroupBys(), node.getOrderBys())) {
                        OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getOrderBys());
                        addHandler(oh);
                    }
                } else if (isOrderNeeded(node, node.getOrderBys())) {
                    OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getOrderBys());
                    addHandler(oh);
                }
            }
        }
        if (node.getLimitTo() >= 0) {
            LimitHandler lh = new LimitHandler(getSequenceId(), session, node.getLimitFrom(), node.getLimitTo());
            addHandler(lh);
        }

    }

    /**
     * add a handler into handler chain
     */
    void addHandler(DMLResponseHandler bh) {
        if (currentLast == null) {
            start = bh;
        } else {
            currentLast.setNextHandler(bh);
        }
        currentLast = bh;
        bh.setAllPushDown(canPushDown);
    }

    /*----------------------------- helper method -------------------*/
    private boolean isNestLoopStrategy(PlanNode planNode) {
        return planNode.type() == PlanNodeType.TABLE && planNode.getNestLoopFilters() != null;
    }

    private void nestLoopAddHandler(SendMakeHandler sh) {
        if (node instanceof TableNode && Objects.nonNull(((TableNode) node).getHintNestLoopHelper())) {
            addDelayTableHandler(sh, (TableNode) node);
        } else if (node instanceof JoinNode && canDoAsMerge()) {
            nestLoopAddHandler(sh, node);
        } else if (node.haveDependOnNode()) {
            List<PlanNode> nestLoopDependOnNodeList = node.getNestLoopDependOnNodeList();
            for (PlanNode planNode : nestLoopDependOnNodeList) {
                TableNode tableNode = (TableNode) planNode.getNestLoopDependNode();
                HintNestLoopHelper hintNestLoopHelper = tableNode.getHintNestLoopHelper();
                List<DelayTableHandler> delayTableHandlers = hintNestLoopHelper.getDelayTableHandlers(tableNode);
                Map<PlanNode, SendMakeHandler> sendMakeHandlerHashMap = hintNestLoopHelper.getSendMakeHandlerHashMap();
                Set<BaseDMLHandler> tableHandlers = sh.getTableHandlers();
                for (DelayTableHandler delayTableHandler : delayTableHandlers) {
                    if (StringUtil.equals(planNode.getAlias(), delayTableHandler.getTableAlias())) {
                        tableHandlers.add(delayTableHandler);
                    }
                }
                sendMakeHandlerHashMap.put(tableNode, sh);
            }
        }
    }

    private void nestLoopAddHandler(SendMakeHandler sh, PlanNode currentNode) {
        if (currentNode instanceof JoinNode) {
            for (PlanNode child : currentNode.getChildren()) {
                if (child instanceof TableNode && Objects.nonNull(((TableNode) child).getHintNestLoopHelper())) {
                    addDelayTableHandler(sh, (TableNode) child);
                } else {
                    nestLoopAddHandler(sh, child);
                }
            }
        }
    }

    private void addDelayTableHandler(SendMakeHandler sh, TableNode tableNode) {
        HintNestLoopHelper hintNestLoopHelper = tableNode.getHintNestLoopHelper();
        List<DelayTableHandler> delayTableHandlers = hintNestLoopHelper.getDelayTableHandlers(tableNode);
        Map<PlanNode, SendMakeHandler> sendMakeHandlerHashMap = hintNestLoopHelper.getSendMakeHandlerHashMap();
        Set<BaseDMLHandler> tableHandlers = sh.getTableHandlers();
        for (DelayTableHandler delayTableHandler : delayTableHandlers) {
            if (!tableHandlers.contains(delayTableHandler)) {
                tableHandlers.add(delayTableHandler);
            }
        }
        sendMakeHandlerHashMap.put(tableNode, sh);
    }

    /**
     * if the node's parent handler has been ordered,it is no need to order again
     */
    private boolean isOrderNeeded(PlanNode planNode, List<Order> orderBys) {
        if (planNode instanceof TableNode || PlanUtil.isGlobalOrER(planNode))
            return false;
        else if (planNode instanceof JoinNode) {
            return !isJoinNodeOrderMatch((JoinNode) planNode, orderBys);
        } else if (planNode instanceof QueryNode) {
            return !isQueryNodeOrderMatch((QueryNode) planNode, orderBys);
        }
        return true;
    }

    /**
     * the order way of join node stored in left join on orders and right join on orders
     */
    private boolean isJoinNodeOrderMatch(JoinNode jn, List<Order> orderBys) {
        // onCondition column in orderBys will be saved to onOrders,
        // eg: if jn.onCond = (t1.id=t2.id),
        // orderBys is t1.id,t2.id,t1.name, and onOrders = {t1.id,t2.id};
        List<Order> leftOnOrders = jn.getLeftJoinOnOrders();
        if (leftOnOrders.size() >= orderBys.size()) {
            return PlanUtil.orderContains(leftOnOrders, orderBys);
        }
        List<Order> onOrdersTest = orderBys.subList(0, leftOnOrders.size());
        if (!PlanUtil.orderContains(leftOnOrders, onOrdersTest)) {
            return false;
        }

        List<Order> pushedOrders = PlanUtil.getPushDownOrders(jn, orderBys.subList(onOrdersTest.size(), orderBys.size()));
        if (jn.isLeftOrderMatch()) {
            List<Order> leftChildOrders = jn.getLeftNode().getOrderBys();
            List<Order> leftRemainOrders = leftChildOrders.subList(leftOnOrders.size(), leftChildOrders.size());
            return PlanUtil.orderContains(leftRemainOrders, pushedOrders);
        }
        return false;
    }

    private boolean isQueryNodeOrderMatch(QueryNode qn, List<Order> orderBys) {
        List<Order> childOrders = qn.getChild().getOrderBys();
        List<Order> pushedOrders = PlanUtil.getPushDownOrders(qn, orderBys);
        return PlanUtil.orderContains(childOrders, pushedOrders);
    }

    /**
     * try to merger the order of 'order by' syntax to columnsSelected
     */
    private List<Order> mergeOrderBy(List<Item> columnsSelected, List<Order> orderBys) {
        List<Integer> orderIndexes = new ArrayList<>();
        List<Order> newOrderByList = new ArrayList<>();
        for (Order orderBy : orderBys) {
            Item column = orderBy.getItem();
            int index = columnsSelected.indexOf(column);
            if (index < 0)
                return null;
            else
                orderIndexes.add(index);
            Order newOrderBy = new Order(columnsSelected.get(index), orderBy.getSortOrder());
            newOrderByList.add(newOrderBy);
        }
        for (int index = 0; index < columnsSelected.size(); index++) {
            if (!orderIndexes.contains(index)) {
                Order newOrderBy = new Order(columnsSelected.get(index), SQLOrderingSpecification.ASC);
                newOrderByList.add(newOrderBy);
            }
        }
        return newOrderByList;
    }

    private static boolean nodeHasGroupBy(PlanNode arg) {
        return (arg.getSumFuncs().size() > 0 || arg.getGroupBys().size() > 0);
    }

    public static long getSequenceId() {
        return SEQUENCE_ID.incrementAndGet();
    }

    void buildMergeHandler(PlanNode planNode, RouteResultsetNode[] rrssArray) {
        hBuilder.checkRRSs(rrssArray);
        List<Order> orderBys = planNode.getGroupBys().size() > 0 ? planNode.getGroupBys() : planNode.getOrderBys();
        boolean isEasyMerge = rrssArray.length == 1 || (orderBys == null || orderBys.size() == 0);

        MultiNodeMergeHandler mh;
        if (isEasyMerge) {
            Set<String> globalBackNodes = null;
            if (planNode.getUnGlobalTableCount() == 0) {
                globalBackNodes = planNode.getNoshardNode();
            }
            mh = new MultiNodeEasyMergeHandler(getSequenceId(), rrssArray, !session.getShardingService().isInTransaction(), session, globalBackNodes);
        } else {
            mh = new MultiNodeMergeAndOrderHandler(getSequenceId(), rrssArray, !session.getShardingService().isInTransaction(), session, orderBys, planNode.haveDependOnNode());
        }
        addHandler(mh);
    }

    BaseTableConfig getTableConfig(String schema, String table) {
        SchemaConfig schemaConfig = schemaConfigMap.get(schema);
        if (schemaConfig == null)
            return null;
        return schemaConfig.getTables().get(table);
    }

    List<DMLResponseHandler> getSubQueriesEndHandlers(List<ItemSubQuery> subQueries) {
        List<DMLResponseHandler> endHandlers = new ArrayList<>(subQueries.size());
        for (ItemSubQuery itemSubQuery : subQueries) {
            if (itemSubQuery instanceof ItemSingleRowSubQuery) {
                SubQueryHandler tempHandler = new SingleRowSubQueryHandler(getSequenceId(), session, (ItemSingleRowSubQuery) itemSubQuery, isExplain);
                DMLResponseHandler endHandler = getSubQueryHandler(itemSubQuery.getPlanNode(), tempHandler);
                endHandlers.add(endHandler);
            } else if (itemSubQuery instanceof ItemInSubQuery) {
                SubQueryHandler tempHandler = new InSubQueryHandler(getSequenceId(), session, (ItemInSubQuery) itemSubQuery, isExplain);
                DMLResponseHandler endHandler = getSubQueryHandler(itemSubQuery.getPlanNode(), tempHandler);
                endHandlers.add(endHandler);
            } else if (itemSubQuery instanceof ItemAllAnySubQuery) {
                final SubQueryHandler tempHandler = new AllAnySubQueryHandler(getSequenceId(), session, (ItemAllAnySubQuery) itemSubQuery, isExplain);
                DMLResponseHandler endHandler = getSubQueryHandler(itemSubQuery.getPlanNode(), tempHandler);
                endHandlers.add(endHandler);
            } else if (itemSubQuery instanceof UpdateItemSubQuery) {
                SubQueryHandler tempHandler = new UpdateSubQueryHandler(getSequenceId(), session, (UpdateItemSubQuery) itemSubQuery, isExplain);
                PlanNode queryNode = ((UpdateItemSubQuery) itemSubQuery).getQueryNode();
                DMLResponseHandler endHandler = getSubQueryHandler(queryNode == null ? itemSubQuery.getPlanNode() : queryNode, tempHandler);
                endHandlers.add(endHandler);
            }
        }
        return endHandlers;
    }

    Set<String> tryRouteWithCurrentNode(Set<String> subQueryRouteNodes, String tmpRouteNode, Set<String> globalNodes) {
        if (subQueryRouteNodes.contains(tmpRouteNode)) {
            subQueryRouteNodes.clear();
            subQueryRouteNodes.add(tmpRouteNode);
        } else if (globalNodes != null) {
            subQueryRouteNodes.retainAll(globalNodes);
        } else {
            subQueryRouteNodes.clear();
        }
        return subQueryRouteNodes;
    }

    Set<String> tryRouteSubQueries(List<DMLResponseHandler> subQueryEndHandlers) {
        List<DMLResponseHandler> merges = new ArrayList<>();
        for (DMLResponseHandler endHandler : subQueryEndHandlers) {
            merges.addAll(endHandler.getMerges());
            if (!isExplain) {
                // outer table use other cond(not sub-query cond) to route
                ((SubQueryHandler) (endHandler.getNextHandler())).setForExplain();
            }
        }
        return HandlerBuilder.canRouteToNodes(merges);
    }


    void buildMergeHandlerWithSubQueries(List<DMLResponseHandler> subQueryEndHandlers, Set<String> queryRouteNodes) {

        for (DMLResponseHandler endHandler : subQueryEndHandlers) {
            // clearExplain status
            SubQueryHandler subQueryHandler = (SubQueryHandler) (endHandler.getNextHandler());
            if (!isExplain) {
                subQueryHandler.clearForExplain();
            }
        }
        GlobalVisitor visitor = new GlobalVisitor(node, true, false);
        visitor.visit();
        String sql = visitor.getSql().toString();
        Map<String, String> mapTableToSimple = visitor.getMapTableToSimple();
        for (Map.Entry<String, String> tableToSimple : mapTableToSimple.entrySet()) {
            sql = sql.replace(tableToSimple.getKey(), tableToSimple.getValue());
        }
        RouteResultset realRrs = new RouteResultset(sql, ServerParse.SELECT);
        realRrs.setStatement(sql);
        realRrs.setComplexSQL(true);
        buildOneMergeHandler(queryRouteNodes, realRrs);
    }

    void buildOneMergeHandler(Set<String> queryRouteNodes, RouteResultset realRrs) {
        String routeNode;
        if (queryRouteNodes.size() == 1) {
            routeNode = queryRouteNodes.iterator().next();
        } else {
            routeNode = RouterUtil.getRandomShardingNode(queryRouteNodes);
        }
        RouterUtil.routeToSingleNode(realRrs, routeNode, null);
        // route to routeNode and backup:subQueryRouteNodes
        if (queryRouteNodes.size() > 1) {
            node.setUnGlobalTableCount(0);
            node.setNoshardNode(queryRouteNodes);
        } else {
            node.setNoshardNode(null);
        }
        node.setSingleRoute(true);
        buildMergeHandler(node, realRrs.getNodes());
    }


    boolean tryRouteToOneNode(List<DMLResponseHandler> pres) {
        List<DMLResponseHandler> merges = Lists.newArrayList();
        for (DMLResponseHandler preHandler : pres) {
            List<DMLResponseHandler> baseMerges = preHandler.getMerges();
            if (HandlerBuilder.nestLoopCheck(baseMerges)) {
                return false;
            }
            merges.addAll(baseMerges);
        }
        Set<String> routeNodes = HandlerBuilder.canRouteToNodes(merges);
        if (routeNodes != null && routeNodes.size() > 0) {
            this.needCommon = false;
            GlobalVisitor visitor = new GlobalVisitor(node, true, true);
            visitor.visit();
            String sql = visitor.getSql().toString();
            Map<String, String> mapTableToSimple = visitor.getMapTableToSimple();
            for (Map.Entry<String, String> tableToSimple : mapTableToSimple.entrySet()) {
                sql = sql.replace(tableToSimple.getKey(), tableToSimple.getValue());
            }
            RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
            rrs.setStatement(sql);
            rrs.setComplexSQL(true);
            buildOneMergeHandler(routeNodes, rrs);
            return true;
        }
        return false;
    }


    boolean handleSubQueries() throws SQLException {
        if (node.getSubQueries().size() != 0) {
            List<DMLResponseHandler> subQueryEndHandlers;
            subQueryEndHandlers = getSubQueriesEndHandlers(node.getSubQueries());
            Set<String> subQueryRouteNodes = tryRouteSubQueries(subQueryEndHandlers);
            if (subQueryRouteNodes != null && tryBuildWithCurrentNode(subQueryEndHandlers, subQueryRouteNodes)) {
                subQueryBuilderList.clear();
                return true;
            }
            if (!isExplain) {
                // execute subquery sync
                executeSubQueries(subQueryEndHandlers);
            }
        }
        return false;
    }

    private DMLResponseHandler getSubQueryHandler(PlanNode planNode, SubQueryHandler tempHandler) {
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, planNode, isExplain);
        DMLResponseHandler endHandler = builder.getEndHandler();
        if (endHandler != null)
            endHandler.setNextHandler(tempHandler);
        subQueryBuilderList.add(builder);
        return endHandler;
    }

    private void subQueryFinished(AtomicInteger subNodes, ReentrantLock lock, AtomicBoolean finished, Condition finishSubQuery) {
        if (subNodes.decrementAndGet() == 0) {
            lock.lock();
            try {
                finished.set(true);
                finishSubQuery.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    public PlanNode getNode() {
        return node;
    }

    public boolean isExistView() {
        return subQueryBuilderList.stream().anyMatch(BaseHandlerBuilder::isExistView) || node.isExistView();
    }

    public boolean isFastBack() {
        return isFastBack;
    }

    public boolean isContainSubQuery(PlanNode planNode) {
        return planNode.getSubQueries().size() > 0 || planNode.getChildren().stream().anyMatch(this::isContainSubQuery);
    }
}
