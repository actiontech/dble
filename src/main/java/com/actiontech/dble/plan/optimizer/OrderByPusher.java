/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * push down merge/join's order by ,contains implicit order by,eg:groupBy
 *
 * @author ActionTech 2015-07-10
 */
public final class OrderByPusher {
    private OrderByPusher() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-order-by");
        try {
            qtn = preOrderByPusher(qtn);
            qtn = pushOrderBy(qtn);
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }

    private static PlanNode preOrderByPusher(PlanNode qtn) {
        if (PlanUtil.isGlobalOrER(qtn)) {
            return qtn;
        }
        for (PlanNode child : qtn.getChildren()) {
            preOrderByPusher(child);
        }
        buildImplicitOrderBys(qtn);
        return qtn;
    }

    private static PlanNode pushOrderBy(PlanNode qtn) {
        if (PlanUtil.isGlobalOrER(qtn))
            return qtn;
        if (qtn.type() == PlanNodeType.MERGE) {
            // note:do not execute mergenode
            for (PlanNode child : qtn.getChildren()) {
                pushOrderBy(child);
            }
            return qtn;
        } else if (qtn.type() == PlanNodeType.JOIN) {
            JoinNode join = (JoinNode) qtn;

            // sort merge join's order by, need to push down to left/right node
            List<Order> implicitOrders = getOrderBysGroupFirst(join);
            boolean canMatch = getJoinColumnOrders(join.getJoinFilter(), join.getLeftJoinOnOrders(),
                    join.getRightJoinOnOrders(), implicitOrders);
            boolean leftOrderPushSuc = false;
            boolean rightOrderPushSuc = false;
            if (canMatch) {
                // push down join column first
                leftOrderPushSuc = tryPushOrderToChild(join, join.getLeftJoinOnOrders(), join.getLeftNode());
                if (leftOrderPushSuc) {
                    tryPushOrderToChild(join, implicitOrders, join.getLeftNode());
                }
                rightOrderPushSuc = tryPushOrderToChild(join, join.getRightJoinOnOrders(), join.getRightNode());
                if (rightOrderPushSuc) {
                    tryPushOrderToChild(join, implicitOrders, join.getRightNode());
                }
            } else {
                leftOrderPushSuc = tryPushOrderToChild(join, join.getLeftJoinOnOrders(), join.getLeftNode());
                rightOrderPushSuc = tryPushOrderToChild(join, join.getRightJoinOnOrders(), join.getRightNode());
            }
            join.setLeftOrderMatch(leftOrderPushSuc);
            join.setRightOrderMatch(rightOrderPushSuc);
        } else if (qtn.type() == PlanNodeType.QUERY) {
            // push order to subQuery
            QueryNode query = (QueryNode) qtn;
            tryPushOrderToChild(query, getOrderBysGroupFirst(query), query.getChild());
        }

        for (PlanNode child : qtn.getChildren()) {
            if (child instanceof PlanNode) {
                pushOrderBy(child);
            }
        }

        return qtn;
    }

    /**
     * generatejoinOnFilters,if joinOn's orderBy can change to match implicitOrders ,return true
     *
     * @param joinOnFilters  in
     * @param leftOnOrders   out
     * @param rightOnOrders  out
     * @param implicitOrders in
     * @return
     */
    private static boolean getJoinColumnOrders(List<ItemFuncEqual> joinOnFilters, List<Order> leftOnOrders,
                                               List<Order> rightOnOrders, List<Order> implicitOrders) {
        List<Item> leftOnSels = new ArrayList<>();
        List<Item> rightOnSels = new ArrayList<>();
        for (ItemFuncEqual bf : joinOnFilters) {
            leftOnSels.add(bf.arguments().get(0));
            rightOnSels.add(bf.arguments().get(1));
        }
        //is on's orderBy can be changed to match implicitOrders
        Map<Integer, SQLOrderingSpecification> foundOnIndexs = new LinkedHashMap<>();
        for (Order orderby : implicitOrders) {
            Item orderSel = orderby.getItem();
            int index = -1;
            if ((index = leftOnSels.indexOf(orderSel)) >= 0) {
                foundOnIndexs.put(index, orderby.getSortOrder());
            } else if ((index = rightOnSels.indexOf(orderSel)) >= 0) {
                foundOnIndexs.put(index, orderby.getSortOrder());
            } else {
                // neither belong to leftOn nor belong to rightOn
                break;
            }
        }
        boolean canMatch = false;
        if (implicitOrders.size() < leftOnSels.size()) {
            if (foundOnIndexs.size() == implicitOrders.size()) { // all join order by is found in leftOnsel
                // left on order =  join order + other join sel
                for (int index = 0; index < leftOnSels.size(); index++) {
                    foundOnIndexs.putIfAbsent(index, SQLOrderingSpecification.ASC);
                }
                canMatch = true;
            }
        } else {
            if (foundOnIndexs.size() == leftOnSels.size()) {
                canMatch = true;
            }
        }
        if (canMatch) {
            for (Map.Entry<Integer, SQLOrderingSpecification> entry : foundOnIndexs.entrySet()) {
                int foundOnIndex = entry.getKey();
                SQLOrderingSpecification sortOrder = entry.getValue();
                Item leftOn = leftOnSels.get(foundOnIndex);
                Item rightOn = rightOnSels.get(foundOnIndex);
                // add lefton order
                Order leftOnOrder = new Order(leftOn, sortOrder);
                leftOnOrders.add(leftOnOrder);
                // add righton order
                Order rightOnOrder = new Order(rightOn, sortOrder);
                rightOnOrders.add(rightOnOrder);
            }
            return true;
        }
        // can not match
        for (int index = 0; index < leftOnSels.size(); index++) {
            SQLOrderingSpecification sortOrder = SQLOrderingSpecification.ASC;
            Item leftOn = leftOnSels.get(index);
            Item rightOn = rightOnSels.get(index);
            // add lefton order
            Order leftOnOrder = new Order(leftOn, sortOrder);
            leftOnOrders.add(leftOnOrder);
            // add righton order
            Order rightOnOrder = new Order(rightOn, sortOrder);
            rightOnOrders.add(rightOnOrder);
        }
        return false;
    }

    /**
     * tryPushOrderToChild
     *
     * @param pOrders
     * @param child
     * @return false, if child's orders not match pOrders
     */
    private static boolean tryPushOrderToChild(PlanNode parent, List<Order> pOrders, PlanNode child) {
        for (Order pOrder : pOrders) {
            if (!PlanUtil.canPush(pOrder.getItem(), child, parent))
                return false;
        }
        List<Order> pushedOrders = PlanUtil.getPushDownOrders(parent, pOrders);
        List<Order> childImplicitOrders = child.getOrderBys();
        boolean childOrderContains = PlanUtil.orderContains(childImplicitOrders, pushedOrders);
        if (child.getLimitTo() == -1) {
            // if child's orders more than parent's orders,keep child's ,or use parent's
            if (!childOrderContains)
                child.setOrderBys(pushedOrders);
            return true;
        } else {
            // has limit,order by can not be push down
            return childOrderContains;
        }
    }

    /**
     * generated node's order by. eg:no order by but exist groupby ,add orderby
     */
    private static void buildImplicitOrderBys(PlanNode node) {
        // if  has order by,
        List<Order> newOrderBys = new ArrayList<>();
        if (!node.getOrderBys().isEmpty()) {
            if (!node.getGroupBys().isEmpty()) {
                // is order by contains group by
                for (Order orderBy : node.getOrderBys()) {
                    if (findOrderByByColumn(node.getGroupBys(), orderBy.getItem()) != null) {
                        newOrderBys.add(orderBy.copy());
                    } else {
                        if (newOrderBys.size() == node.getGroupBys().size()) {
                            // contains
                            node.setGroupBys(newOrderBys); // reorder group by
                        } else {
                            return;
                        }
                    }
                }
                for (Order groupBy : node.getGroupBys()) {
                    if (findOrderByByColumn(newOrderBys, groupBy.getItem()) == null) {
                        // add field which is not in order by
                        newOrderBys.add(groupBy.copy());
                    }
                }
                node.setGroupBys(newOrderBys); // reorder group by
                node.setOrderBys(newOrderBys);
            }
        } else {
            // no order by,copy group by
            if (!node.getGroupBys().isEmpty()) {
                for (Order orderBy : node.getGroupBys()) {
                    newOrderBys.add(orderBy.copy());
                }
            }
            node.setOrderBys(newOrderBys);
        }
    }

    /**
     * try to find an column which has the same name to the order by
     */
    private static Order findOrderByByColumn(List<Order> orderbys, Item column) {
        for (Order order : orderbys) {
            if (order.getItem().equals(column)) {
                return order;
            }
        }

        return null;
    }

    /**
     * getOrderBysGroupFirst
     *
     * @param node
     * @return
     */
    private static List<Order> getOrderBysGroupFirst(PlanNode node) {
        List<Order> groupBys = node.getGroupBys();
        List<Order> orderBys = node.getOrderBys();
        if (groupBys.isEmpty()) {
            return orderBys;
        } else if (orderBys.isEmpty()) {
            return groupBys;
        } else if (PlanUtil.orderContains(orderBys, groupBys)) {
            return orderBys;
        } else
            return groupBys;
    }

}
