/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCond;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemFuncNot;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemInSubQuery;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemScalarSubQuery;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemSubQuery;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

public final class SubQueryPreNoTransformProcessor {
    private SubQueryPreNoTransformProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("for-subquery");
        try {
            MergeHavingFilter.optimize(qtn);
            handlerComparisonsSubQuery(qtn);
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }

    /**
     * http://dev.mysql.com/doc/refman/5.0/en/comparisons-using-subqueries.html
     */
    private static PlanNode handlerComparisonsSubQuery(PlanNode qtn) {
        for (int i = 0; i < qtn.getChildren().size(); i++) {
            PlanNode child = qtn.getChildren().get(i);
            qtn.getChildren().set(i, handlerComparisonsSubQuery(child));
        }
        buildColumnsSelectSubQuery(qtn);
        buildMayJoinSubQuery(qtn);
        buildOrderSubQuery(qtn);
        //having contains sub query
        buildSubQuery(qtn, qtn.getHavingFilter());
        buildSubQuery(qtn, qtn.getWhereFilter());
        return qtn;
    }

    private static void buildMayJoinSubQuery(PlanNode qtn) {
        if (qtn instanceof JoinNode) {
            //join on filter sub query
            buildSubQuery(qtn, ((JoinNode) qtn).getOtherJoinOnFilter());
        }
    }

    private static void buildColumnsSelectSubQuery(PlanNode qtn) {
        qtn.getColumnsSelected().stream().filter(itemSelect -> itemSelect instanceof ItemSubQuery).
                forEach(itemSelect -> handlerComparisonsSubQuery(((ItemSubQuery) itemSelect).getPlanNode()));
    }

    private static void buildOrderSubQuery(PlanNode node) {
        for (Order o : node.getOrderBys()) {
            if (o.getItem() instanceof ItemScalarSubQuery) {
                node.getSubQueries().add((ItemScalarSubQuery) o.getItem());
            }
        }
    }

    private static void buildSubQuery(PlanNode node, Item filter) {
        if (filter == null || !filter.isWithSubQuery()) {
            return;
        }
        if (filter instanceof ItemCond) {
            buildSubQueryWithCondFilter(node, (ItemCond) filter);
        } else if (filter instanceof ItemFuncNot) {
            buildSubQueryWithNotFilter(node, (ItemFuncNot) filter);
        } else if (filter instanceof ItemInSubQuery) {
            buildSubQueryByFilter(node, filter);
        } else {
            addSubQueryForExpr(node, filter);
        }
    }

    private static void buildSubQueryByFilter(PlanNode node, Item filter) {
        addSubQuery(node, (ItemInSubQuery) filter);
    }

    private static void addSubQueryForExpr(PlanNode node, Item filter) {
        if (filter instanceof ItemSubQuery) {
            addSubQuery(node, (ItemSubQuery) filter);
        } else if (filter instanceof ItemFunc) {
            filter.arguments().stream().filter(Item::isWithSubQuery).forEach(subQuery -> addSubQueryForExpr(node, subQuery));
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
        }
    }

    private static void addSubQuery(PlanNode node, ItemSubQuery subQuery) {
        node.getSubQueries().add(subQuery);
        PlanNode subNode = handlerComparisonsSubQuery(subQuery.getPlanNode());
        if (subQuery instanceof ItemInSubQuery) {
            subNode.setDistinct(true);
        }
        subQuery.setPlanNode(subNode);
    }

    private static void buildSubQueryWithCondFilter(PlanNode node, ItemCond filter) {
        filter.arguments().forEach(SubQueryFilter -> buildSubQuery(node, SubQueryFilter));
    }

    private static void buildSubQueryWithNotFilter(PlanNode node, ItemFuncNot
            filter) {
        buildSubQuery(node, filter.arguments().get(0));
    }
}
