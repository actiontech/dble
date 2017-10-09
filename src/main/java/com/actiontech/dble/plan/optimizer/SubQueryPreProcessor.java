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
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.subquery.*;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;
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

        SubQueryFilter find = new SubQueryFilter();
        find.query = qtn;
        find.filter = null;
        Item where = qtn.getWhereFilter();
        SubQueryFilter result = buildSubQuery(qtn, find, where, false);
        if (result != find) {
            // that means where filter only contains sub query,just replace it
            result.query.query(result.filter);
            qtn.query(null);
            // change result.filter and rebuild
            result.query.setUpFields();
            return result.query;
        } else {
            return qtn;
        }
    }

    private static SubQueryFilter buildSubQuery(PlanNode node, SubQueryFilter qtn, Item filter, boolean isOrChild) {
        if (filter == null)
            return qtn;
        if (!filter.isWithSubQuery()) {
            qtn.filter = filter;
        } else if (filter instanceof ItemCondOr) {
            return buildSubQueryWithOrFilter(node, qtn, (ItemCondOr) filter);
        } else if (filter instanceof ItemCondAnd) {
            return buildSubQueryWithAndFilter(node, qtn, (ItemCondAnd) filter, isOrChild);
        } else {
            return buildSubQueryByFilter(node, qtn, filter, isOrChild);
        }
        return qtn;
    }

    private static SubQueryFilter buildSubQueryByFilter(PlanNode node, SubQueryFilter qtn, Item filter, boolean isOrChild) {
        if (filter instanceof ItemInSubQuery && !isOrChild) {
            return transformInSubQuery(qtn, (ItemInSubQuery) filter);
        } else if (filter instanceof ItemInSubQuery) {
            addSubQuey(node, (ItemInSubQuery) filter);
            return qtn;
        } else if (PlanUtil.isCmpFunc(filter)) {
            ItemBoolFunc2 eqFilter = (ItemBoolFunc2) filter;
            Item arg0 = eqFilter.arguments().get(0);
            if (arg0.type().equals(ItemType.SUBSELECT_ITEM)) {
                if (arg0 instanceof ItemScalarSubQuery) {
                    addSubQuey(node, ((ItemScalarSubQuery) arg0));
                } else {
                    //todo: when happened?
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
                }
            }

            Item arg1 = eqFilter.arguments().get(1);
            if (arg1.type().equals(ItemType.SUBSELECT_ITEM)) {
                if (arg1 instanceof ItemScalarSubQuery) {
                    addSubQuey(node, ((ItemScalarSubQuery) arg1));
                } else if (arg1 instanceof ItemAllAnySubQuery) {
                    addSubQuey(node, ((ItemAllAnySubQuery) arg1));
                } else {
                    //todo: when happened?
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
                }
            }
            return qtn;
        } else if (filter.type().equals(ItemType.SUBSELECT_ITEM)) {
            if (filter instanceof ItemExistsSubQuery) {
                addSubQuey(node, ((ItemExistsSubQuery) filter));
            } else {
                //todo: when happened?
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
            }
            return qtn;
        } else {
            //todo: when happened?
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
        }
    }

    private static void addSubQuey(PlanNode node, ItemSubQuery subQuery) {
        node.getSubQueries().add(subQuery);
        PlanNode subNode = findComparisonsSubQueryToJoinNode(subQuery.getPlanNode());
        subQuery.setPlanNode(subNode);
    }

    private static SubQueryFilter transformInSubQuery(SubQueryFilter qtn, ItemInSubQuery filter) {
        Item leftColumn = filter.getLeftOperand();
        PlanNode query = filter.getPlanNode();
        query = findComparisonsSubQueryToJoinNode(query);
        if (StringUtils.isEmpty(query.getAlias()))
            query.alias(AUTOALIAS + query.getPureName());
        if (query.getColumnsSelected().size() != 1)
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "only support subquery of one column");
        query.setSubQuery(true).setDistinct(true);

        final List<Item> newSelects = qtn.query.getColumnsSelected();
        SubQueryFilter result = new SubQueryFilter();
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
        // leave origin sql to new join node
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
        if (filter.isNeg()) {
            ((JoinNode) result.query).setLeftOuterJoin().setNotIn(true);
            ItemFuncEqual joinFilter = FilterUtils.equal(leftColumn, rightJoinColumn);
            ((JoinNode) result.query).addJoinFilter(joinFilter);
            result.filter = null;
        } else {
            Item joinFilter = FilterUtils.equal(leftColumn, rightJoinColumn);
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

    private static SubQueryFilter buildSubQueryWithOrFilter(PlanNode node, SubQueryFilter qtn, ItemCondOr filter) {
        for (int index = 0; index < filter.getArgCount(); index++) {
            buildSubQuery(node, qtn, filter.arguments().get(index), true);
        }
        return qtn;
    }

    private static SubQueryFilter buildSubQueryWithAndFilter(PlanNode node, SubQueryFilter qtn, ItemCondAnd filter, boolean isOrChild) {
        for (int index = 0; index < filter.getArgCount(); index++) {
            SubQueryFilter result = buildSubQuery(node, qtn, filter.arguments().get(index), isOrChild);
            if (result != qtn) {
                if (result.filter == null) {
                    result.filter = new ItemInt(1);
                }
                filter.arguments().set(index, result.filter);
                qtn = result;
            }
        }
        qtn.filter = filter;
        return qtn;
    }

    private static class SubQueryFilter {

        PlanNode query; // subQuery may change query node to join node
        Item filter; // sub query's filter
    }

}
