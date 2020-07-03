/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * push down the filter which may contains ER KEY
 */
public final class FilterJoinColumnPusher {
    private FilterJoinColumnPusher() {
    }


    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-re-push-down");
        try {
            qtn = pushFilter(qtn, new ArrayList<Item>());
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }

    private static PlanNode pushFilter(PlanNode qtn, List<Item> dnfNodeToPush) {
        // the leaf node receive  filter as where , or merge the current where and push down
        if (qtn.getChildren().isEmpty()) {
            Item node = FilterUtils.and(dnfNodeToPush);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
            }
            return qtn;
        }

        Item filterInWhere = qtn.getWhereFilter();
        //left/right join: where filter can't be push to child
        if (filterInWhere != null && (!(qtn.type() == PlanNode.PlanNodeType.JOIN) || ((JoinNode) qtn).isInnerJoin())) {
            List<Item> splits = FilterUtils.splitFilter(filterInWhere);
            List<Item> nonJoinFilter = new ArrayList<>();
            for (Item filter : splits) {
                if (!isPossibleERJoinColumnFilter(qtn, filter)) {
                    nonJoinFilter.add(filter);
                } else {
                    dnfNodeToPush.add(filter);
                }
            }
            if (nonJoinFilter.size() != splits.size()) {
                //rollbakc nonJoinFilter
                qtn.query(FilterUtils.and(nonJoinFilter));
            }
        }
        PlanNode.PlanNodeType i = qtn.type();
        if (i == PlanNode.PlanNodeType.QUERY) {
            return pushQueryNodeFilter(qtn, dnfNodeToPush);
        } else if (i == PlanNode.PlanNodeType.JOIN) {
            return pushJoinNodeFilter(qtn, dnfNodeToPush);
        } else if (i == PlanNode.PlanNodeType.MERGE) {
            return pushMergeNodeFilter(qtn);
        }
        return qtn;
    }

    private static PlanNode pushMergeNodeFilter(PlanNode qtn) {
        List<PlanNode> children = qtn.getChildren();
        for (PlanNode aChildren : children) {
            pushFilter(aChildren, new ArrayList<Item>());
        }
        return qtn;
    }

    private static PlanNode pushJoinNodeFilter(PlanNode qtn, List<Item> dnfNodeToPush) {
        JoinNode jn = (JoinNode) qtn;
        PlanUtil.findJoinColumnsAndRemoveIt(dnfNodeToPush, jn);
        if (dnfNodeToPush.isEmpty()) {
            return qtn;
        }
        // filters which can not push down
        List<Item> dnfNodeToCurrent = new LinkedList<>();
        List<Item> dnfNodetoPushToLeft = new LinkedList<>();
        List<Item> dnfNodetoPushToRight = new LinkedList<>();
        for (Item filter : dnfNodeToPush) {
            if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
                dnfNodetoPushToLeft.add(filter);
            } else if (PlanUtil.canPush(filter, jn.getRightNode(), jn)) {
                dnfNodetoPushToRight.add(filter);
            } else {
                dnfNodeToCurrent.add(filter);
            }
        }
        // if can not push down,merge to current where
        Item node = FilterUtils.and(dnfNodeToCurrent);
        if (node != null) {
            qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
        }
        if (jn.isInnerJoin()) {
            refreshPdFilters(jn, dnfNodetoPushToLeft);
            refreshPdFilters(jn, dnfNodetoPushToRight);
            pushFilter(jn.getLeftNode(), dnfNodetoPushToLeft);
            pushFilter(((JoinNode) qtn).getRightNode(), dnfNodetoPushToRight);
        } else if (jn.isLeftOuterJoin()) {
            refreshPdFilters(jn, dnfNodetoPushToLeft);
            pushFilter(jn.getLeftNode(), dnfNodetoPushToLeft);
            if (!dnfNodeToPush.isEmpty()) {
                jn.query(FilterUtils.and(dnfNodetoPushToRight)); // the parent's filter,don't push down
            }
        } else if (jn.isRightOuterJoin()) {
            refreshPdFilters(jn, dnfNodetoPushToRight);
            pushFilter(((JoinNode) qtn).getRightNode(), dnfNodetoPushToRight);
            if (!dnfNodeToPush.isEmpty()) {
                jn.query(FilterUtils.and(dnfNodetoPushToLeft)); //  the parent's filter,don't push down
            }
        } else {
            if (!dnfNodeToPush.isEmpty()) {
                jn.query(FilterUtils.and(dnfNodeToPush));
            }
        }
        return qtn;
    }

    private static PlanNode pushQueryNodeFilter(PlanNode qtn, List<Item> dnfNodeToPush) {
        if (dnfNodeToPush.isEmpty()) {
            return qtn;
        }
        refreshPdFilters(qtn, dnfNodeToPush);
        PlanNode child = pushFilter(qtn.getChild(), dnfNodeToPush);
        ((QueryNode) qtn).setChild(child);
        return qtn;
    }

    /**
     * is ER Filter: 1.Filter must be equal(=) 2.Filter must be Column = Column
     * 3.Filter's key and value must be belong different table ex:a.id=b.id true a.id=b.id+1 false
     */
    private static boolean isPossibleERJoinColumnFilter(PlanNode node, Item ifilter) {
        if (!(ifilter instanceof ItemFuncEqual))
            return false;
        ItemFuncEqual filter = (ItemFuncEqual) ifilter;
        Item column = filter.arguments().get(0);
        Item value = filter.arguments().get(1);
        if (column != null && column instanceof ItemField && value != null && value instanceof ItemField) {
            Pair<TableNode, ItemField> foundColumn = PlanUtil.findColumnInTableLeaf((ItemField) column, node);
            Pair<TableNode, ItemField> foundValue = PlanUtil.findColumnInTableLeaf((ItemField) value, node);
            if (foundColumn != null && foundValue != null) {
                String columnTable = foundColumn.getValue().getTableName();
                String valueTable = foundValue.getValue().getTableName();
                // the table must be different
                return !StringUtils.equals(columnTable, valueTable);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private static void refreshPdFilters(PlanNode qtn, List<Item> filters) {
        for (int index = 0; index < filters.size(); index++) {
            Item toPsFilter = filters.get(index);
            Item pdFilter = PlanUtil.pushDownItem(qtn, toPsFilter);
            filters.set(index, pdFilter);
        }
    }

}
