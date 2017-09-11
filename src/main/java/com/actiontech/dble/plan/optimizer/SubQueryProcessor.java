/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.view.ViewUtil;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * change View,change query node to other three type node
 *
 * @author ActionTech
 */
public final class SubQueryProcessor {
    private SubQueryProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        BoolPtr merged = new BoolPtr(false);
        qtn = tryTransformerQuery(qtn, merged);
        if (merged.get())
            qtn.setUpFields();
        return qtn;
    }

    /**
     * find query node in qtn ,change to other 3 type node
     *
     * @param qtn
     * @return
     */
    private static PlanNode tryTransformerQuery(PlanNode qtn, BoolPtr boolptr) {
        boolean childMerged = false;
        for (int index = 0; index < qtn.getChildren().size(); index++) {
            PlanNode child = qtn.getChildren().get(index);
            BoolPtr cbptr = new BoolPtr(false);
            PlanNode newChild = tryTransformerQuery(child, cbptr);
            if (cbptr.get())
                childMerged = true;
            qtn.getChildren().set(index, newChild);
        }
        if (childMerged)
            qtn.setUpFields();
        if (qtn.type() == PlanNodeType.QUERY) {
            qtn = transformerQuery((QueryNode) qtn, boolptr);
        }
        return qtn;
    }

    /**
     * transformerQuery
     *
     * @param query
     * @return
     */
    private static PlanNode transformerQuery(QueryNode query, BoolPtr boolptr) {
        boolean canBeMerged = ViewUtil.canBeMerged(query.getChild());
        if (canBeMerged) {
            // merge viewnode's property to view's child
            PlanNode newNode = mergeNode(query, query.getChild());
            boolptr.set(true);
            return newNode;
        } else {
            return query;
        }
    }

    /**
     * merge parent's property to child,and return new child,
     * of course ,the child is  canBeMerged
     *
     * @param parent
     * @param child
     * @return
     */
    private static PlanNode mergeNode(PlanNode parent, PlanNode child) {
        final List<Item> newSels = mergeSelect(parent, child);
        mergeWhere(parent, child);
        mergeGroupBy(parent, child);
        mergeHaving(parent, child);
        mergeOrderBy(parent, child);
        mergeLimit(parent, child);
        child.setColumnsSelected(newSels);
        if (!StringUtils.isEmpty(parent.getAlias()))
            child.setAlias(parent.getAlias());
        else if (!StringUtils.isEmpty(parent.getSubAlias()))
            child.setAlias(parent.getSubAlias());
        child.setSubQuery(parent.isSubQuery());
        child.setParent(parent.getParent());
        return child;
    }

    /**
     * view v_t1 as select id+1 idd from t1 tt1 order by idd select view
     * sql:select idd + 1 from v_t1 ==> select (id+1) + 1 from t1 tt1 order by
     * id+1
     *
     * @return child should contains new select's infos
     * @notice
     */

    private static List<Item> mergeSelect(PlanNode parent, PlanNode child) {
        List<Item> pSels = parent.getColumnsSelected();
        List<Item> cNewSels = new ArrayList<>();
        for (Item pSel : pSels) {
            Item pSel0 = PlanUtil.pushDownItem(parent, pSel, true);
            String selName = pSel.getAlias();
            if (StringUtils.isEmpty(selName)) {
                selName = pSel.getItemName();
                // parent is func and func has no alias,mysql not allow select func() as func()
                if (pSel.type() == ItemType.FUNC_ITEM || pSel.type() == ItemType.COND_ITEM ||
                        pSel.type() == ItemType.SUM_FUNC_ITEM)
                    selName = Item.FNAF + selName;
            }
            pSel0.setAlias(selName);
            cNewSels.add(pSel0);
        }
        return cNewSels;
    }

    private static void mergeWhere(PlanNode parent, PlanNode child) {
        Item pWhere = parent.getWhereFilter();
        Item pWhere0 = PlanUtil.pushDownItem(parent, pWhere, true);
        Item mWhere = FilterUtils.and(pWhere0, child.getWhereFilter());
        child.setWhereFilter(mWhere);
    }

    private static void mergeGroupBy(PlanNode parent, PlanNode child) {
        List<Order> pGroups = parent.getGroupBys();
        List<Order> cGroups = new ArrayList<>();
        for (Order pGroup : pGroups) {
            Item col = pGroup.getItem();
            Item col0 = PlanUtil.pushDownItem(parent, col);
            Order pGroup0 = new Order(col0, pGroup.getSortOrder());
            cGroups.add(pGroup0);
        }
        child.setGroupBys(cGroups);
    }

    private static void mergeHaving(PlanNode parent, PlanNode child) {
        Item pHaving = parent.getHavingFilter();
        Item pHaving0 = PlanUtil.pushDownItem(parent, pHaving, true);
        Item mHaving = FilterUtils.and(pHaving0, child.getHavingFilter());
        child.having(mHaving);
    }

    private static void mergeOrderBy(PlanNode parent, PlanNode child) {
        List<Order> pOrders = parent.getOrderBys();
        List<Order> cOrders = new ArrayList<>();
        for (Order pOrder : pOrders) {
            Item col = pOrder.getItem();
            Item col0 = PlanUtil.pushDownItem(parent, col, true);
            Order pOrder0 = new Order(col0, pOrder.getSortOrder());
            cOrders.add(pOrder0);
        }
        if (cOrders.size() > 0)
            child.setOrderBys(cOrders);
    }

    private static void mergeLimit(PlanNode parent, PlanNode child) {
        child.setLimitFrom(parent.getLimitFrom());
        child.setLimitTo(parent.getLimitTo());
    }

}
