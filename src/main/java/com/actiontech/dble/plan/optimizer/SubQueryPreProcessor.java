/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.subquery.ItemInSubselect;
import com.actiontech.dble.plan.common.item.subquery.ItemSubselect;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.util.FilterUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public final class SubQueryPreProcessor {
    private SubQueryPreProcessor() {
    }

    private static final String AUTONAME = "autosubgenrated0";
    private static final String AUTOALIAS = "autoalias_";

    public static PlanNode optimize(PlanNode qtn) {
        MergeHavingFilter.optimize(qtn);
        qtn = findComparisonsSubQueryToJoinNode(qtn);
        return qtn;
    }

    /**
     * http://dev.mysql.com/doc/refman/5.0/en/comparisons-using-subqueries.html
     */
    private static PlanNode findComparisonsSubQueryToJoinNode(PlanNode qtn) {
        for (int i = 0; i < qtn.getChildren().size(); i++) {
            PlanNode child = qtn.getChildren().get(i);
            qtn.getChildren().set(i, findComparisonsSubQueryToJoinNode(child));
        }

        SubQueryAndFilter find = new SubQueryAndFilter();
        find.query = qtn;
        find.filter = null;
        Item where = qtn.getWhereFilter();
        SubQueryAndFilter result = buildSubQuery(find, where);
        if (result != find) {
            // that means where filter only contains subquery,just replace it
            result.query.query(result.filter);
            qtn.query(null);
            // change result.filter and rebuild
            result.query.setUpFields();
            return result.query;
        } else {
            return qtn;
        }
    }

    private static SubQueryAndFilter buildSubQuery(SubQueryAndFilter qtn, Item filter) {
        if (filter == null)
            return qtn;
        if (!filter.isWithSubQuery()) {
            qtn.filter = filter;
        } else if (filter instanceof ItemCondOr) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support 'or' when condition subquery");
        } else if (filter instanceof ItemCondAnd) {
            return buildSubQueryWithAndFilter(qtn, (ItemCondAnd) filter);
        } else {
            return buildSubQueryByFilter(qtn, filter);
        }
        return qtn;
    }

    private static SubQueryAndFilter buildSubQueryByFilter(SubQueryAndFilter qtn, Item filter) {
        Item leftColumn;
        PlanNode query;
        boolean isNotIn = false;
        boolean needExchange = false;
        if (isCmpFunc(filter)) {
            ItemBoolFunc2 eqFilter = (ItemBoolFunc2) filter;
            Item arg0 = eqFilter.arguments().get(0);
            Item arg1 = eqFilter.arguments().get(1);
            boolean arg0IsSubQuery = arg0.type().equals(ItemType.SUBSELECT_ITEM);
            boolean arg1IsSubQuery = arg1.type().equals(ItemType.SUBSELECT_ITEM);
            if (arg0IsSubQuery && arg1IsSubQuery) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "left and right both condition subquery,not supported...");
            }
            needExchange = arg0IsSubQuery;
            leftColumn = arg0IsSubQuery ? arg1 : arg0;
            query = arg0IsSubQuery ? ((ItemSubselect) arg0).getPlanNode() : ((ItemSubselect) arg1).getPlanNode();
        } else if (filter instanceof ItemInSubselect) {
            ItemInSubselect inSub = (ItemInSubselect) filter;
            leftColumn = inSub.getLeftOprand();
            query = inSub.getPlanNode();
            isNotIn = inSub.isNeg();
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
        }
        query = findComparisonsSubQueryToJoinNode(query);
        if (StringUtils.isEmpty(query.getAlias()))
            query.alias(AUTOALIAS + query.getPureName());
        if (query.getColumnsSelected().size() != 1)
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "only support subquery of one column");
        query.setSubQuery(true).setDistinct(true);

        final List<Item> newSelects = qtn.query.getColumnsSelected();
        SubQueryAndFilter result = new SubQueryAndFilter();
        Item rightColumn = query.getColumnsSelected().get(0);
        qtn.query.setColumnsSelected(new ArrayList<Item>());
        String rightJoinName = rightColumn.getAlias();
        if (StringUtils.isEmpty(rightJoinName)) {
            if (rightColumn instanceof ItemField) {
                rightJoinName = rightColumn.getItemName();
            } else {
                rightColumn.setAlias(AUTONAME);
                rightJoinName = AUTONAME;
            }
        }

        ItemField rightJoinColumn = new ItemField(null, query.getAlias(), rightJoinName);
        // rename the left column's table name
        result.query = new JoinNode(qtn.query, query);
        // leave origin sqlto new join node
        result.query.setSql(qtn.query.getSql());
        qtn.query.setSql(null);
        result.query.select(newSelects);
        if (!qtn.query.getOrderBys().isEmpty()) {
            List<Order> orderBys = new ArrayList<>();
            orderBys.addAll(qtn.query.getOrderBys());
            result.query.setOrderBys(orderBys);
            qtn.query.getOrderBys().clear();
        }
        if (!qtn.query.getGroupBys().isEmpty()) {
            List<Order> groupBys = new ArrayList<>();
            groupBys.addAll(qtn.query.getGroupBys());
            result.query.setGroupBys(groupBys);
            qtn.query.getGroupBys().clear();
        }
        if (qtn.query.getLimitFrom() != -1) {
            result.query.setLimitFrom(qtn.query.getLimitFrom());
            qtn.query.setLimitFrom(-1);
        }
        if (qtn.query.getLimitTo() != -1) {
            result.query.setLimitTo(qtn.query.getLimitTo());
            qtn.query.setLimitTo(-1);
        }
        if (isNotIn) {
            ((JoinNode) result.query).setLeftOuterJoin().setNotIn(true);
            ItemFuncEqual joinFilter = FilterUtils.equal(leftColumn, rightJoinColumn);
            ((JoinNode) result.query).addJoinFilter(joinFilter);
            result.filter = null;
        } else {
            Item joinFilter = calcJoinFilter(filter, leftColumn, needExchange, rightJoinColumn);
            result.query.query(joinFilter);
            result.filter = joinFilter;
        }
        if (qtn.query.getAlias() == null && qtn.query.getSubAlias() == null) {
            result.query.setAlias(qtn.query.getPureName());
        } else {
            String queryAlias = qtn.query.getAlias();
            qtn.query.alias(null);
            if (queryAlias == null) {
                queryAlias = qtn.query.getSubAlias();
            }
            result.query.setAlias(queryAlias);
        }
        result.query.setUpFields();
        return result;
    }

    private static boolean isCmpFunc(Item filter) {
        return filter instanceof ItemFuncEqual || filter instanceof ItemFuncGt || filter instanceof ItemFuncGe ||
                filter instanceof ItemFuncLt || filter instanceof ItemFuncLe || filter instanceof ItemFuncNe ||
                filter instanceof ItemFuncStrictEqual;
    }

    private static Item calcJoinFilter(Item filter, Item leftColumn, boolean needExchange, ItemField rightJoinColumn) {
        Item joinFilter;
        if (((filter instanceof ItemFuncGt) && !needExchange) ||
                ((filter instanceof ItemFuncLt) && needExchange)) {
            joinFilter = FilterUtils.greaterThan(leftColumn, rightJoinColumn);
        } else if (((filter instanceof ItemFuncLt) && !needExchange) ||
                ((filter instanceof ItemFuncGt) && needExchange)) {
            joinFilter = FilterUtils.lessThan(leftColumn, rightJoinColumn);
        } else if (((filter instanceof ItemFuncGe) && !needExchange) ||
                ((filter instanceof ItemFuncLe) && needExchange)) {
            joinFilter = FilterUtils.greaterEqual(leftColumn, rightJoinColumn);
        } else if (((filter instanceof ItemFuncLe) && !needExchange) ||
                ((filter instanceof ItemFuncGe) && needExchange)) {
            joinFilter = FilterUtils.lessEqual(leftColumn, rightJoinColumn);
        } else if (filter instanceof ItemFuncNe) {
            joinFilter = FilterUtils.notEqual(leftColumn, rightJoinColumn);
        } else {
            //equal or in
            joinFilter = FilterUtils.equal(leftColumn, rightJoinColumn);
        }
        return joinFilter;
    }

    private static SubQueryAndFilter buildSubQueryWithAndFilter(SubQueryAndFilter qtn, ItemCondAnd filter) {
        ItemCondAnd andFilter = filter;
        for (int index = 0; index < andFilter.getArgCount(); index++) {
            SubQueryAndFilter result = buildSubQuery(qtn, andFilter.arguments().get(index));
            if (result != qtn) {
                if (result.filter == null) {
                    result.filter = new ItemInt(1);
                }
                andFilter.arguments().set(index, result.filter);
                qtn = result;
            }
        }
        qtn.filter = andFilter;
        return qtn;
    }

    private static class SubQueryAndFilter {

        PlanNode query; // subQuery may change querynode to join node
        Item filter; // sub query's filter
    }

}
