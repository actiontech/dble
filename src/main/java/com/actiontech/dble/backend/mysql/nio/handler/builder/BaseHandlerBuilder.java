/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.GlobalVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.*;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.OrderedGroupByHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

abstract class BaseHandlerBuilder {
    private static AtomicLong sequenceId = new AtomicLong(0);
    protected NonBlockingSession session;
    protected HandlerBuilder hBuilder;
    protected DMLResponseHandler start;
    /* the current last handler */
    protected DMLResponseHandler currentLast;
    private PlanNode node;
    protected ServerConfig config;
    /* the children can be push down */
    protected boolean canPushDown = false;
    /* need common handler? like group by,order by,limit and so on */
    protected boolean needCommon = true;
    /* has where handler */
    protected boolean needWhereHandler = true;
    /* it's no need to send maker if sql is just the same as the client origin query  */
    protected boolean needSendMaker = true;

    protected BaseHandlerBuilder(NonBlockingSession session, PlanNode node, HandlerBuilder hBuilder) {
        this.session = session;
        this.node = node;
        this.hBuilder = hBuilder;
        this.config = DbleServer.getInstance().getConfig();
        if (config.getSchemas().isEmpty())
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "current router config is empty!");
    }

    public DMLResponseHandler getEndHandler() {
        return currentLast;
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
        } else if (!node.isExsitView() && PlanUtil.isGlobal(node)) {
            // the query can be send to a certain node
            noShardBuild();
        } else if (canDoAsMerge()) {
            // the query can be send to some certain nodes .eg: ER tables,  GLOBAL*NORMAL GLOBAL*ER
            mergeBuild();
        } else {
            //need to split to simple query
            preHandlers = buildPre();
            buildOwn();
        }
        if (needCommon)
            buildCommon();
        if (needSendMaker) {
            // view subalias
            String tbAlias = node.getAlias();
            if (node.getParent() != null && node.getParent().getSubAlias() != null)
                tbAlias = node.getParent().getSubAlias();
            SendMakeHandler sh = new SendMakeHandler(getSequenceId(), session, node.getColumnsSelected(), tbAlias);
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

    protected abstract List<DMLResponseHandler> buildPre();

    /**
     * build own handler
     */
    protected abstract void buildOwn();

    /**
     * no shard-ing node,just send to the first node
     */
    protected final void noShardBuild() {
        this.needCommon = false;
        // nearly all global tables :unGlobalCount=0.
        // Maybe the join node break the rule. eg: global1(node 0,1) join global2(node 2,3)
        String sql = null;
        if (node.getParent() == null) { // it's root
            sql = node.getSql();
        }
        // maybe some node is view
        if (sql == null) {
            GlobalVisitor visitor = new GlobalVisitor(node, true);
            visitor.visit();
            sql = visitor.getSql().toString();
        } else {
            needSendMaker = false;
        }
        RouteResultsetNode[] rrss = getTableSources(node.getNoshardNode(), sql);
        hBuilder.checkRRSs(rrss);
        MultiNodeMergeHandler mh = new MultiNodeMergeHandler(getSequenceId(), rrss, session.getSource().isAutocommit() && !session.getSource().isTxstart(),
                session, null);
        addHandler(mh);
    }

    /**
     * build common properties,like where,groupby,having,orderby,limit, and sendMakHandler(rename)
     */
    protected void buildCommon() {
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
                if (funRef.hasWithDistinct() || funRef.sumType().equals(ItemSum.Sumfunctype.GROUP_CONCAT_FUNC))
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
                    OrderedGroupByHandler gh = new OrderedGroupByHandler(getSequenceId(), session, node.getGroupBys(),
                            sumRefs);
                    addHandler(gh);
                }
            } else { // @bug 1052 canDirectGroupby condition we use
                // directgroupby already
                OrderedGroupByHandler gh = new OrderedGroupByHandler(getSequenceId(), session, node.getGroupBys(),
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
        if (node.getLimitTo() > 0) {
            LimitHandler lh = new LimitHandler(getSequenceId(), session, node.getLimitFrom(), node.getLimitTo());
            addHandler(lh);
        }

    }

    /**
     * add a handler into handler chain
     */
    protected void addHandler(DMLResponseHandler bh) {
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
     * @param planNode
     * @param orderBys
     * @return
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
     * @param jn
     * @param orderBys
     * @return
     */
    private boolean isJoinNodeOrderMatch(JoinNode jn, List<Order> orderBys) {
        // onCondition column in orderBys will be saved to onOrders,
        // eg: if jn.onCond = (t1.id=t2.id),
        // orderBys is t1.id,t2.id,t1.name, and onOrders = {t1.id,t2.id};
        List<Order> onOrders = new ArrayList<>();
        List<Order> leftOnOrders = jn.getLeftJoinOnOrders();
        List<Order> rightOnOrders = jn.getRightJoinOnOrders();
        for (Order orderBy : orderBys) {
            if (leftOnOrders.contains(orderBy) || rightOnOrders.contains(orderBy)) {
                onOrders.add(orderBy);
            } else {
                break;
            }
        }
        if (onOrders.isEmpty()) {
            // join node must order by joinOnCondition
            return false;
        } else {
            List<Order> remainOrders = orderBys.subList(onOrders.size(), orderBys.size());
            if (remainOrders.isEmpty()) {
                return true;
            } else {
                List<Order> pushedOrders = PlanUtil.getPushDownOrders(jn, remainOrders);
                if (jn.isLeftOrderMatch()) {
                    List<Order> leftChildOrders = jn.getLeftNode().getOrderBys();
                    List<Order> leftRemainOrders = leftChildOrders.subList(leftOnOrders.size(), leftChildOrders.size());
                    if (PlanUtil.orderContains(leftRemainOrders, pushedOrders))
                        return true;
                } else if (jn.isRightOrderMatch()) {
                    List<Order> rightChildOrders = jn.getRightNode().getOrderBys();
                    List<Order> rightRemainOrders = rightChildOrders.subList(rightOnOrders.size(),
                            rightChildOrders.size());
                    if (PlanUtil.orderContains(rightRemainOrders, pushedOrders))
                        return true;
                }
                return false;
            }
        }
    }

    /**
     * @param qn
     * @param orderBys
     * @return
     */
    private boolean isQueryNodeOrderMatch(QueryNode qn, List<Order> orderBys) {
        List<Order> childOrders = qn.getChild().getOrderBys();
        List<Order> pushedOrders = PlanUtil.getPushDownOrders(qn, orderBys);
        return PlanUtil.orderContains(childOrders, pushedOrders);
    }

    /**
     * try to merger the order of 'order by' syntax to columnsSelected
     *
     * @param columnsSelected
     * @param orderBys
     * @return
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

    protected static boolean nodeHasGroupBy(PlanNode arg) {
        return (arg.getSumFuncs().size() > 0 || arg.getGroupBys().size() > 0);
    }

    protected static long getSequenceId() {
        return sequenceId.incrementAndGet();
    }

    protected void buildMergeHandler(PlanNode planNode, RouteResultsetNode[] rrssArray) {
        hBuilder.checkRRSs(rrssArray);
        MultiNodeMergeHandler mh = null;
        List<Order> orderBys = planNode.getGroupBys().size() > 0 ? planNode.getGroupBys() : planNode.getOrderBys();

        mh = new MultiNodeMergeHandler(getSequenceId(), rrssArray, session.getSource().isAutocommit() && !session.getSource().isTxstart(), session,
                orderBys);
        addHandler(mh);
    }

    protected RouteResultsetNode[] getTableSources(Set<String> dataNodes, String sql) {
        String randomDatenode = null;
        int index = (int) (System.currentTimeMillis() % dataNodes.size());
        int i = 0;
        for (String dataNode : dataNodes) {
            if (index == i) {
                randomDatenode = dataNode;
                break;
            }
            i++;
        }
        RouteResultsetNode rrss = new RouteResultsetNode(randomDatenode, ServerParse.SELECT, sql);
        return new RouteResultsetNode[]{rrss};
    }

    protected TableConfig getTableConfig(String schema, String table) {
        SchemaConfig schemaConfig = this.config.getSchemas().get(schema);
        if (schemaConfig == null)
            return null;
        return schemaConfig.getTables().get(table);
    }

}
