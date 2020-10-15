/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * push down filter
 * <p>
 * <pre>
 * a. if conditions contains || ,then stop pushing down ,otherwise it is not correct
 * b. if conditions's column/value contains function,then stop pushing down
 * (TODO: CHECK FIELD IN FUNCTION TO DECIDE IF IT CAN BE PUSHED)
 * c.if conditions's column/value is from child's function,then stop pushing down
 *
 * cases: 1. where pushed to leaf, get the joinFilter
 * node type: JoinNode/QueryNode
 * Notice:if JoinNode is outter,stop pushing down
 *
 * eg: tabl1.join(table2).query(
 * "table1.id>5 && table2.id<10 && table1.name = table2.name")
 * after optimized:
 * table1.query("table1.id>5").join(table2.query("table2.id<10").on(
 * "table1.name = table2.name")
 *
 * eg: table1.join(table2).query("table1.id = table2.id")
 * after optimized:
 * :table1.join(table2).on("table1.id = table2.id")
 *
 * 2. join's constant condition,like column = 1,push to leaf
 * node type:JoinNode
 *
 * eg: tabl1.join(table2).on("table1.id>5&&table2.id<10")
 * after optimized:
 * table1.query("table1.id>5").join(table2.query("table2.id<10")) t
 *
 *
 * 3. both 1 and 2
 * node type:JoinNode
 *
 * eg: table.join(table2).on(
 * "table1.id = table2.id and table1.id>5 && table2.id<10")
 *  after optimized::table1.query(
 * "table1.id>5 && table1.id<10").join(table2.query(
 * "table2.id>5 && table2.id<10")).on("table1.id = table2.id")
 */
public final class FilterPusher {
    private FilterPusher() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-filter-push-down");
        try {
            mergeJoinOnFilter(qtn);
            qtn = pushJoinOnFilter(qtn);
            qtn = pushFilter(qtn, new ArrayList<>());
            if (qtn instanceof NoNameNode && qtn.isContainsSubQuery()) {
                for (ItemSubQuery subQuery : qtn.getSubQueries()) {
                    PlanNode subQtn = optimize(subQuery.getPlanNode());
                    subQuery.setPlanNode(subQtn);
                }
            }
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }

    /**
     * merge inner joi's otheron to where if we can
     *
     */
    private static void mergeJoinOnFilter(PlanNode qtn) {
        if (PlanUtil.isGlobalOrER(qtn))
            return;
        if (qtn.type().equals(PlanNodeType.JOIN) && ((JoinNode) qtn).isInnerJoin()) {
            JoinNode jn = (JoinNode) qtn;
            Item otherJoinOn = jn.getOtherJoinOnFilter();
            jn.setOtherJoinOnFilter(null);
            jn.query(FilterUtils.and(otherJoinOn, jn.getWhereFilter()));
        }
        for (PlanNode child : qtn.getChildren()) {
            mergeJoinOnFilter(child);
        }
    }

    private static PlanNode pushFilter(PlanNode qtn, List<Item> dnfNodeToPush) {
        List<Item> subHavingList = new ArrayList<>();
        for (Item filter : dnfNodeToPush) {
            if (filter.isWithSumFunc()) {
                subHavingList.add(filter);
            }
        }
        if (!subHavingList.isEmpty()) {
            qtn.having(FilterUtils.and(qtn.getHavingFilter(), FilterUtils.and(subHavingList)));
            dnfNodeToPush.removeAll(subHavingList);
        }

        // root node will receive filter as where ,otherwise merge the current where and push down
        if (qtn.getChildren().isEmpty() || (PlanUtil.isGlobalOrER(qtn) && qtn.type() != PlanNodeType.MERGE)) {
            Item node = FilterUtils.and(dnfNodeToPush);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
                qtn.setContainsSubQuery(qtn.getWhereFilter().isWithSubQuery() || isWithSubQuery(dnfNodeToPush));
            }
            return qtn;
        }

        Item filterInWhere = qtn.getWhereFilter();
        //left/right join: where filter can't be push to child
        if (filterInWhere != null) {
            List<Item> splits = FilterUtils.splitFilter(filterInWhere);
            qtn.query(null);
            dnfNodeToPush.addAll(splits);
        }

        if (qtn.type() == PlanNodeType.QUERY) {
            return getQueryNode(qtn, dnfNodeToPush);
        } else if (qtn.type() == PlanNodeType.JOIN) {
            return getJoinNode(qtn, dnfNodeToPush);
        } else if (qtn.type() == PlanNodeType.MERGE) {
            return getMergeNode(qtn, dnfNodeToPush);
        }

        return qtn;
    }

    // qtn is a query node, only has one child
    private static PlanNode getQueryNode(PlanNode qtn, List<Item> dnfNodeToPush) {
        if (qtn.getSubQueries().size() > 0) {
            Item node = FilterUtils.and(dnfNodeToPush);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
                qtn.setContainsSubQuery(qtn.getWhereFilter().isWithSubQuery() || isWithSubQuery(dnfNodeToPush));
            }
            return qtn;
        }

        refreshPdFilters(qtn, dnfNodeToPush);
        PlanNode child = pushFilter(qtn.getChild(), dnfNodeToPush);
        ((QueryNode) qtn).setChild(child);
        return qtn;
    }

    private static PlanNode getMergeNode(PlanNode qtn, List<Item> dnfNodeToPush) {
        // union's where can be pushed down ,but it must be replace to child's condition

        Item node = FilterUtils.and(dnfNodeToPush);
        if (node != null) {
            qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
            qtn.setContainsSubQuery(qtn.getWhereFilter().isWithSubQuery() || isWithSubQuery(dnfNodeToPush));
        }
        Item mergeWhere = qtn.getWhereFilter();
        // push down merge's condition
        qtn.query(null);
        List<Item> pushFilters = PlanUtil.getPushItemsToUnionChild((MergeNode) qtn, mergeWhere,
                ((MergeNode) qtn).getColIndexs());
        List<PlanNode> childs = qtn.getChildren();
        for (int index = 0; index < childs.size(); index++) {
            PlanNode child = childs.get(index);
            if (pushFilters != null) {
                Item pushFilter = pushFilters.get(index);
                if (pushFilter.isWithSumFunc()) {
                    child.having(FilterUtils.and(child.getHavingFilter(), pushFilter));
                } else {
                    child.query(FilterUtils.and(child.getWhereFilter(), pushFilter));
                }
            }
            FilterPusher.optimize(child);
        }
        return qtn;
    }

    private static PlanNode getJoinNode(PlanNode qtn, List<Item> dnfNodeToPush) {
        JoinNode jn = (JoinNode) qtn;
        List<Item> dnfNodetoPushToLeft = new LinkedList<>();
        List<Item> dnfNodetoPushToRight = new LinkedList<>();
        List<Item> leftCopyedPushFilters = new LinkedList<>();
        List<Item> rightCopyedPushFilters = new LinkedList<>();
        List<Item> dnfNodeToCurrent = new LinkedList<>();

        PlanUtil.findJoinColumnsAndRemoveIt(dnfNodeToPush, jn);
        for (Item filter : dnfNodeToPush) {
            // ex. 1 = -1
            if (filter.getReferTables().size() == 0) {
                dnfNodetoPushToLeft.add(filter);
                dnfNodetoPushToRight.add(filter);
                continue;
            }
            if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
                dnfNodetoPushToLeft.add(filter);
            } else if (PlanUtil.canPush(filter, jn.getRightNode(), jn)) {
                dnfNodetoPushToRight.add(filter);
            } else {
                dnfNodeToCurrent.add(filter);
            }
        }
        if (jn.isInnerJoin() || jn.isLeftOuterJoin() || jn.isRightOuterJoin()) {
            // push the left expr to join filter's right condition
            rightCopyedPushFilters.addAll(
                    copyFilterToJoinOnColumns(dnfNodetoPushToLeft, jn.getLeftKeys(), jn.getRightKeys()));

            // push the right expr to join filter's left condition
            leftCopyedPushFilters.addAll(
                    copyFilterToJoinOnColumns(dnfNodetoPushToRight, jn.getRightKeys(), jn.getLeftKeys()));
        }

        // if it can not push down, merge to current where
        Item node = FilterUtils.and(dnfNodeToCurrent);
        if (node != null) {
            qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
        }

        if (jn.isInnerJoin() || jn.isLeftOuterJoin() || jn.isRightOuterJoin()) {
            if (jn.isLeftOuterJoin()) {
                // left join, push down the right join ,but still contains right joins condition
                jn.query(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(dnfNodetoPushToRight)));
            }
            if (jn.isRightOuterJoin()) {
                // right join, push down the left join ,but still contains left joins condition
                jn.query(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(dnfNodetoPushToLeft)));
            }
            // merge
            dnfNodetoPushToRight.addAll(rightCopyedPushFilters);
            dnfNodetoPushToLeft.addAll(leftCopyedPushFilters);

            refreshPdFilters(jn, dnfNodetoPushToLeft);
            refreshPdFilters(jn, dnfNodetoPushToRight);
            jn.setLeftNode(pushFilter(jn.getLeftNode(), dnfNodetoPushToLeft));
            jn.setRightNode(pushFilter(((JoinNode) qtn).getRightNode(), dnfNodetoPushToRight));
        } else {
            if (!dnfNodeToPush.isEmpty()) {
                jn.query(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(dnfNodeToPush)));
            }
        }
        return jn;
    }

    /**
     * inner join's other join on would add to where when FilterPre and push down,
     * when Left join, eg" select * from t1 left join t2 on t1.id=t2.id and t1.id = 10 and
     * t2.name = 'aaa' push down t2.id=10 and t2.name='aaa'
     */
    private static PlanNode pushJoinOnFilter(PlanNode qtn) {
        if (PlanUtil.isGlobalOrER(qtn))
            return qtn;
        if (qtn.type().equals(PlanNodeType.JOIN)) {
            JoinNode jn = (JoinNode) qtn;
            Item otherJoinOn = jn.getOtherJoinOnFilter();
            if (jn.isLeftOuterJoin() && otherJoinOn != null) {
                List<Item> pushToRightNode = new ArrayList<>();
                List<Item> splitedFilters = FilterUtils.splitFilter(otherJoinOn);
                for (Item filter : splitedFilters) {
                    if (filter.getReferTables().isEmpty())
                        pushToRightNode.add(filter);
                    else if (PlanUtil.canPush(filter, jn.getRightNode(), jn))
                        pushToRightNode.add(filter);
                    else if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
                        Item copyedFilter = copyFilterToJoinOnColumns(filter, jn.getRightKeys(), jn.getLeftKeys());
                        if (copyedFilter != null)
                            pushToRightNode.add(copyedFilter);
                    }
                }
                if (!pushToRightNode.isEmpty()) {
                    splitedFilters.removeAll(pushToRightNode);
                    Item newOtherJoinOn = FilterUtils.and(splitedFilters);
                    jn.setOtherJoinOnFilter(newOtherJoinOn);
                    refreshPdFilters(jn, pushToRightNode);
                    List<Item> subHavingList = new ArrayList<>();
                    List<Item> subWhereList = new ArrayList<>();
                    for (Item filter : pushToRightNode) {
                        if (filter.isWithSumFunc()) {
                            subHavingList.add(filter);
                        } else {
                            subWhereList.add(filter);
                        }
                    }
                    Item subHaving = FilterUtils.and(subHavingList);
                    Item subWhere = FilterUtils.and(subWhereList);
                    jn.getRightNode().having(FilterUtils.and(jn.getRightNode().getHavingFilter(), subHaving));
                    jn.getRightNode().setWhereFilter(FilterUtils.and(jn.getRightNode().getWhereFilter(), subWhere));
                }
            }
        }
        for (PlanNode child : qtn.getChildren())
            pushJoinOnFilter(child);
        return qtn;
    }

    /**
     * copyFilterToJoinOnColumns
     *
     * @param dnf          DNF filter to be copied
     * @param qnColumns    origin node's join column
     * @param otherColumns otherColumns
     */
    private static List<Item> copyFilterToJoinOnColumns(List<Item> dnf, List<Item> qnColumns, List<Item> otherColumns) {
        List<Item> newIFilterToPush = new LinkedList<>();
        for (Item filter : dnf) {
            Item newFilter = copyFilterToJoinOnColumns(filter, qnColumns, otherColumns);
            if (newFilter != null)
                newIFilterToPush.add(newFilter);
        }
        return newIFilterToPush;
    }

    private static Item copyFilterToJoinOnColumns(Item filter, List<Item> qnColumns, List<Item> otherColumns) {
        if (filter.type().equals(ItemType.FUNC_ITEM) || filter.type().equals(ItemType.COND_ITEM)) {
            ItemFunc newFilter = replaceFunctionArg((ItemFunc) filter, qnColumns, otherColumns);
            if (newFilter != null)
                return newFilter;
        }
        return null;
    }

    private static void refreshPdFilters(PlanNode qtn, List<Item> filters) {
        for (int index = 0; index < filters.size(); index++) {
            Item toPsFilter = filters.get(index);
            Item pdFilter = PlanUtil.pushDownItem(qtn, toPsFilter);
            filters.set(index, pdFilter);
        }
    }

    /**
     * replaceFunctionArg
     *
     * @return if f also contains selectable which is not sels1 ,return null
     */
    private static ItemFunc replaceFunctionArg(ItemFunc f, List<Item> sels1, List<Item> sels2) {
        ItemFunc ret = (ItemFunc) f.cloneStruct();
        boolean hasFieldInFunc = false;
        for (int index = 0; index < ret.getArgCount(); index++) {
            Item arg = ret.arguments().get(index);
            if (arg instanceof ItemFunc) {
                ItemFunc newfArg = replaceFunctionArg((ItemFunc) arg, sels1, sels2);
                if (newfArg == null)
                    return null;
                else
                    ret.arguments().set(index, newfArg);
            } else if (arg instanceof ItemField) {
                hasFieldInFunc = true;
                int tmpIndex = sels1.indexOf(arg);
                if (tmpIndex < 0) {
                    return null;
                } else {
                    Item newArg = sels2.get(tmpIndex);
                    ret.arguments().set(index, newArg.cloneStruct());
                }
            }  // else do nothing;
        }
        if (!hasFieldInFunc) {
            return null;
        }
        ret.setPushDownName(null);
        PlanUtil.refreshReferTables(ret);
        return ret;
    }

    private static boolean isWithSubQuery(List<Item> filters) {
        for (Item filter : filters) {
            if (filter.isWithSubQuery()) {
                return true;
            }
        }
        return false;
    }
}
