/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.GlobalVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.*;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.AggregateHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.AllAnySubQueryHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.InSubQueryHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.SingleRowSubQueryHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.SubQueryHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.CallBackHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.common.item.subquery.ItemAllAnySubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemInSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemSingleRowSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BaseHandlerBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseHandlerBuilder.class);
    private static AtomicLong sequenceId = new AtomicLong(0);
    protected NonBlockingSession session;
    HandlerBuilder hBuilder;
    protected DMLResponseHandler start;
    /* the current last handler */
    private DMLResponseHandler currentLast;
    private PlanNode node;
    Map<String, SchemaConfig> schemaConfigMap = new HashMap<>();
    /* the children can be push down */
    boolean canPushDown = false;
    /* need common handler? like group by,order by,limit and so on */
    boolean needCommon = true;
    /* has where handler */
    boolean needWhereHandler = true;

    protected boolean isExplain = false;
    private List<BaseHandlerBuilder> subQueryBuilderList = new CopyOnWriteArrayList<>();

    protected BaseHandlerBuilder(NonBlockingSession session, PlanNode node, HandlerBuilder hBuilder, boolean isExplain) {
        this.session = session;
        this.node = node;
        this.hBuilder = hBuilder;
        this.isExplain = isExplain;
        this.schemaConfigMap.putAll(DbleServer.getInstance().getConfig().getSchemas());
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
            mergeBuild();
        } else {
            handleSubQueries();
            //need to split to simple query
            preHandlers = buildPre();
            buildOwn();
        }
        if (needCommon) {
            buildCommon();
        }

        // view sub alias
        String tbAlias = node.getAlias();
        String schema = null;
        String table = null;
        if (node.type() == PlanNodeType.TABLE) {
            TableNode tbNode = (TableNode) node;
            schema = tbNode.getSchema();
            table = tbNode.getTableName();
        }
        if (needCommon || node.isWithSubQuery()) {
            SendMakeHandler sh = new SendMakeHandler(getSequenceId(), session, node.getColumnsSelected(), schema, table, tbAlias);
            addHandler(sh);
        }


        if (preHandlers != null) {
            for (DMLResponseHandler preHandler : preHandlers) {
                preHandler.setNextHandler(start);
            }
        }
    }

    protected void nestLoopBuild() {
        throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "not implement yet, node type[" + node.type() + "]");
    }

    /* er join and  global *normal and table node are need not split to simple query*/
    protected boolean canDoAsMerge() {
        return false;
    }

    protected void mergeBuild() {
        //
    }

    protected abstract void handleSubQueries();

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
        GlobalVisitor visitor = new GlobalVisitor(node, true);
        visitor.visit();
        String sql = visitor.getSql().toString();
        Map<String, String> mapTableToSimple = visitor.getMapTableToSimple();
        for (Map.Entry<String, String> tableToSimple : mapTableToSimple.entrySet()) {
            sql = sql.replace(tableToSimple.getKey(), tableToSimple.getValue());
        }
        String randomShardingNode = getRandomNode(node.getNoshardNode());
        RouteResultsetNode rrsNode = new RouteResultsetNode(randomShardingNode, ServerParse.SELECT, sql);
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

        MultiNodeMergeHandler mh = new MultiNodeEasyMergeHandler(getSequenceId(), rrss, session.getShardingService().isAutocommit() && !session.getShardingService().isTxStart(),
                session);
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
            currentLast = bh;
        } else {
            currentLast.setNextHandler(bh);
            currentLast = bh;
        }
        bh.setAllPushDown(canPushDown);
    }

    /*----------------------------- helper method -------------------*/
    private boolean isNestLoopStrategy(PlanNode planNode) {
        return planNode.type() == PlanNodeType.TABLE && planNode.getNestLoopFilters() != null;
    }

    /**
     * if the node's parent handler has been ordered,it is no need to order again
     *
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
     *
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
            if (PlanUtil.orderContains(leftRemainOrders, pushedOrders))
                return true;
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
     *
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
        return sequenceId.incrementAndGet();
    }

    void buildMergeHandler(PlanNode planNode, RouteResultsetNode[] rrssArray) {
        hBuilder.checkRRSs(rrssArray);
        List<Order> orderBys = planNode.getGroupBys().size() > 0 ? planNode.getGroupBys() : planNode.getOrderBys();
        boolean isEasyMerge = rrssArray.length == 1 || (orderBys == null || orderBys.size() == 0);

        MultiNodeMergeHandler mh;
        if (isEasyMerge) {
            mh = new MultiNodeEasyMergeHandler(getSequenceId(), rrssArray, session.getShardingService().isAutocommit() && !session.getShardingService().isTxStart(), session);
        } else {
            mh = new MultiNodeMergeAndOrderHandler(getSequenceId(), rrssArray, session.getShardingService().isAutocommit() && !session.getShardingService().isTxStart(), session, orderBys);
        }
        addHandler(mh);
    }

    String getRandomNode(Set<String> shardingNodes) {
        String randomDatenode = null;
        int index = (int) (System.currentTimeMillis() % shardingNodes.size());
        int i = 0;
        for (String shardingNode : shardingNodes) {
            if (index == i) {
                randomDatenode = shardingNode;
                break;
            }
            i++;
        }
        return randomDatenode;
    }

    BaseTableConfig getTableConfig(String schema, String table) {
        SchemaConfig schemaConfig = schemaConfigMap.get(schema);
        if (schemaConfig == null)
            return null;
        return schemaConfig.getTables().get(table);
    }

    void handleBlockingSubQuery() {
        if (node.getSubQueries().size() == 0) {
            return;
        }
        final ReentrantLock lock = new ReentrantLock();
        final Condition finishSubQuery = lock.newCondition();
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicInteger subNodes = new AtomicInteger(node.getSubQueries().size());
        final CopyOnWriteArrayList<ErrorPacket> errorPackets = new CopyOnWriteArrayList<>();
        for (ItemSubQuery itemSubQuery : node.getSubQueries()) {
            if (itemSubQuery instanceof ItemSingleRowSubQuery) {
                final SubQueryHandler tempHandler = new SingleRowSubQueryHandler(getSequenceId(), session, (ItemSingleRowSubQuery) itemSubQuery);
                if (isExplain) {
                    handleSubQueryForExplain(lock, finishSubQuery, finished, subNodes, itemSubQuery.getPlanNode(), tempHandler);
                } else {
                    handleSubQuery(lock, finishSubQuery, finished, subNodes, errorPackets, itemSubQuery.getPlanNode(), tempHandler);
                }
            } else if (itemSubQuery instanceof ItemInSubQuery) {
                final SubQueryHandler tempHandler = new InSubQueryHandler(getSequenceId(), session, (ItemInSubQuery) itemSubQuery);
                if (isExplain) {
                    handleSubQueryForExplain(lock, finishSubQuery, finished, subNodes, itemSubQuery.getPlanNode(), tempHandler);
                } else {
                    handleSubQuery(lock, finishSubQuery, finished, subNodes, errorPackets, itemSubQuery.getPlanNode(), tempHandler);
                }
            } else if (itemSubQuery instanceof ItemAllAnySubQuery) {
                final SubQueryHandler tempHandler = new AllAnySubQueryHandler(getSequenceId(), session, (ItemAllAnySubQuery) itemSubQuery);
                if (isExplain) {
                    handleSubQueryForExplain(lock, finishSubQuery, finished, subNodes, itemSubQuery.getPlanNode(), tempHandler);
                } else {
                    handleSubQuery(lock, finishSubQuery, finished, subNodes, errorPackets, itemSubQuery.getPlanNode(), tempHandler);
                }
            }
        }
        lock.lock();
        try {
            while (!finished.get()) {
                finishSubQuery.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("execute ScalarSubQuery " + e);
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

    private void handleSubQueryForExplain(final ReentrantLock lock, final Condition finishSubQuery, final AtomicBoolean finished,
                                          final AtomicInteger subNodes, final PlanNode planNode, final SubQueryHandler tempHandler) {
        tempHandler.setForExplain();
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, planNode, true);
        DMLResponseHandler endHandler = builder.getEndHandler();
        endHandler.setNextHandler(tempHandler);
        this.getSubQueryBuilderList().add(builder);
        subQueryFinished(subNodes, lock, finished, finishSubQuery);
    }

    private void handleSubQuery(final ReentrantLock lock, final Condition finishSubQuery, final AtomicBoolean finished,
                                final AtomicInteger subNodes, final CopyOnWriteArrayList<ErrorPacket> errorPackets, final PlanNode planNode, final SubQueryHandler tempHandler) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                boolean startHandler = false;
                try {
                    BaseHandlerBuilder builder = hBuilder.getBuilder(session, planNode, false);
                    DMLResponseHandler endHandler = builder.getEndHandler();
                    endHandler.setNextHandler(tempHandler);
                    getSubQueryBuilderList().add(builder);
                    CallBackHandler tempDone = new CallBackHandler() {
                        @Override
                        public void call() throws Exception {
                            if (tempHandler.getErrorPacket() != null) {
                                errorPackets.add(tempHandler.getErrorPacket());
                            }
                            subQueryFinished(subNodes, lock, finished, finishSubQuery);
                        }
                    };
                    tempHandler.setTempDoneCallBack(tempDone);
                    startHandler = true;
                    HandlerBuilder.startHandler(endHandler);
                } catch (Exception e) {
                    LOGGER.info("execute ItemScalarSubQuery error", e);
                    ErrorPacket errorPackage = new ErrorPacket();
                    errorPackage.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
                    String errorMsg = e.getMessage() == null ? e.toString() : e.getMessage();
                    errorPackage.setMessage(errorMsg.getBytes(StandardCharsets.UTF_8));
                    errorPackets.add(errorPackage);
                    if (!startHandler) {
                        subQueryFinished(subNodes, lock, finished, finishSubQuery);
                    }
                }
            }
        });
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

}
