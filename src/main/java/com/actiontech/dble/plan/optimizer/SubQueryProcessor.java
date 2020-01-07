/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.node.QueryNode;
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
     */
    private static PlanNode tryTransformerQuery(PlanNode qtn, BoolPtr boolptr) {
        boolean childMerged = false;
        for (int index = 0; index < qtn.getChildren().size(); index++) {
            PlanNode child = qtn.getChildren().get(index);
            BoolPtr boolPtr = new BoolPtr(false);
            PlanNode newChild = tryTransformerQuery(child, boolPtr);
            if (boolPtr.get())
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
     */
    private static PlanNode transformerQuery(QueryNode query, BoolPtr boolptr) {
        boolean canBeMerged = canBeMerged(query.getChild());
        if (canBeMerged) {
            // merge view node's property to view's child
            PlanNode newNode = mergeNode(query, query.getChild());
            boolptr.set(true);
            return newNode;
        } else {
            return query;
        }
    }

    private static boolean canBeMerged(PlanNode viewSelNode) {
        if (viewSelNode.type() == PlanNode.PlanNodeType.NONAME)
            return true;
        boolean selectsAllowMerge = viewSelNode.type() != PlanNode.PlanNodeType.MERGE;
        // TODO as the same as LEX::can_be_merged();
        boolean existAggregate = PlanUtil.existAggregate(viewSelNode);
        return selectsAllowMerge && viewSelNode.getReferedTableNodes().size() == 1 && !existAggregate && viewSelNode.getOrderBys().size() == 0;
    }

    /**
     * merge parent's property to child,and return new child,
     * of course ,the child is  canBeMerged
     *
     */
    private static PlanNode mergeNode(QueryNode parent, PlanNode child) {
        mergeSelect(parent, child);
        mergeWhere(parent, child);
        child.setAlias(parent.getAlias());
        child.setParent(parent.getParent());
        return child;
    }

    /**
     * view v_t1 as select id+1 idd from t1 tt1 order by idd select view
     * sql:select idd + 1 from v_t1 ==> select (id+1) + 1 from t1 tt1 order by
     * id+1
     *
     */

    //fixme: parent's alias set to select item's table
    private static void mergeSelect(PlanNode parent, PlanNode child) {
        List<Item> pSelects = parent.getColumnsSelected();
        List<Item> cNewSelects = new ArrayList<>();
        for (Item pSel : pSelects) {
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
            cNewSelects.add(pSel0);
        }
        child.setColumnsSelected(cNewSelects);
    }
    //fixme: parent's alias set to where item's table
    private static void mergeWhere(PlanNode parent, PlanNode child) {
        Item pWhere = parent.getWhereFilter();
        Item pWhere0 = PlanUtil.pushDownItem(parent, pWhere, true);
        Item childWhere = PlanUtil.pushDownItem(child, child.getWhereFilter(), true);
        Item mWhere = FilterUtils.and(pWhere0, childWhere);
        child.setWhereFilter(mWhere);
    }
}
