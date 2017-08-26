package io.mycat.plan.optimizer;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.util.PlanUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将merge/join中的order by条件下推,包括隐式的order by条件,比如将groupBy转化为orderBy
 *
 * @author ActionTech 2015-07-10
 */
public final class OrderByPusher {
    private OrderByPusher() {
    }

    /**
     * 详细优化见类描述
     */
    public static PlanNode optimize(PlanNode qtn) {
        qtn = preOrderByPusher(qtn);
        qtn = pushOrderBy(qtn);
        return qtn;
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
            // note:目前不做mergenode的处理
            for (PlanNode child : qtn.getChildren()) {
                pushOrderBy(child);
            }
            return qtn;
        } else if (qtn.type() == PlanNodeType.JOIN) {
            JoinNode join = (JoinNode) qtn;

            // sort merge join中的order by,需要推到左/右节点
            List<Order> implicitOrders = getOrderBysGroupFirst(join);
            boolean canMatch = getJoinColumnOrders(join.getJoinFilter(), join.getLeftJoinOnOrders(),
                    join.getRightJoinOnOrders(), implicitOrders);
            boolean leftOrderPushSuc = false;
            boolean rightOrderPushSuc = false;
            if (canMatch) {
                // match的时候,先推join列,在推全部
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
            // 可以将order推到子查询
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
     * 生成joinOnFilters,如果能够调整joinOn的orderBy的顺序并且和implicitOrders顺序吻合,返回true
     *
     * @param joinOnCondition in
     * @param leftOnOrders    out
     * @param rightOnOrders   out
     * @param implicitOrders  in
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
        // 是否可以通过调整on的顺序使得on的orderbys和implicitorders相同
        boolean canMatch = false;
        if (implicitOrders.size() < leftOnSels.size())
            canMatch = false;
        else {
            Map<Integer, SQLOrderingSpecification> foundOnIndexs = new LinkedHashMap<>();
            for (Order orderby : implicitOrders) {
                Item orderSel = orderby.getItem();
                int index = -1;
                if ((index = leftOnSels.indexOf(orderSel)) >= 0) {
                    foundOnIndexs.put(index, orderby.getSortOrder());
                } else if ((index = rightOnSels.indexOf(orderSel)) >= 0) {
                    foundOnIndexs.put(index, orderby.getSortOrder());
                } else {
                    // 既不属于leftOn,也不属于rightOn,结束
                    break;
                }
            }
            if (foundOnIndexs.size() == leftOnSels.size()) {
                canMatch = true;
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
                return canMatch;
            }
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
        return canMatch;
    }

    /**
     * 尝试将orders下推到child中去
     *
     * @param pOrders
     * @param child
     * @return 最终失败,child的orders和pOrders不吻合
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
            // 如果child的orders多余parent的orders,保留child的,否则,以parent的为准
            if (!childOrderContains)
                child.setOrderBys(pushedOrders);
            return true;
        } else {
            // 存在limit时,不能下推order by
            return childOrderContains;
        }
    }

    /**
     * 生成该node的最终数据的orderby. 例如不存在orderby属性,但是存在groupby属性时,手动添加上orderby属性
     */
    private static void buildImplicitOrderBys(PlanNode node) {
        // 如果用户没指定order by,则显示index的order by
        List<Order> newOrderBys = new ArrayList<>();
        if (!node.getOrderBys().isEmpty()) {
            if (!node.getGroupBys().isEmpty()) {
                // 首先以order by的顺序,查找group by中对应的字段
                for (Order orderBy : node.getOrderBys()) {
                    if (findOrderByByColumn(node.getGroupBys(), orderBy.getItem()) != null) {
                        newOrderBys.add(orderBy.copy());
                    } else {
                        if (newOrderBys.size() == node.getGroupBys().size()) {
                            // 说明出现order by包含了整个group by
                            node.setGroupBys(newOrderBys); // 将group by重置一下顺序
                        } else {
                            return;
                        }
                    }
                }
                for (Order groupBy : node.getGroupBys()) {
                    if (findOrderByByColumn(newOrderBys, groupBy.getItem()) == null) {
                        // 添加下order by中没有的字段
                        newOrderBys.add(groupBy.copy());
                    }
                }
                node.setGroupBys(newOrderBys); // 将group by重置一下顺序
                node.setOrderBys(newOrderBys);
            }
        } else {
            // 没有orderby,复制group by
            if (!node.getGroupBys().isEmpty()) {
                for (Order orderBy : node.getGroupBys()) {
                    newOrderBys.add(orderBy.copy());
                }
            }
            node.setOrderBys(newOrderBys);
        }
    }

    /**
     * 尝试查找一个同名的排序字段
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
     * 以groupby为优先的order by list
     *
     * @param preOrderPushedNode
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
