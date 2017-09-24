/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.util.PlanUtil;

import java.util.*;

/**
 * make select as fewer as possible
 *
 * @author ActionTech
 */
public final class SelectedProcessor {
    private SelectedProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        qtn = pushSelected(qtn, new HashSet<Item>());
        return qtn;
    }

    /**
     * pushSelected
     * if parent's  selected refered isA.id,A.name,B.id,B.name,
     * node of A only need push A.id,A.nam
     *
     * @param qtn
     * @param toPushColumns
     * @return
     */
    private static PlanNode pushSelected(PlanNode qtn, Collection<Item> toPushColumns) {
        boolean isPushDownNode = false;
        if (PlanUtil.isGlobalOrER(qtn)) {
            // TODO:buildColumnRefers for every child
            List<Item> selList = qtn.getColumnsSelected();
            for (Item pdSel : toPushColumns) {
                if (!selList.contains(pdSel)) {
                    selList.add(pdSel);
                }
            }
            isPushDownNode = true;
            qtn.setUpRefers(isPushDownNode);
            return qtn;
        }
        isPushDownNode = (qtn.type() == PlanNode.PlanNodeType.TABLE || qtn.type() == PlanNode.PlanNodeType.NONAME);
        if (qtn.type() == PlanNode.PlanNodeType.MERGE) {
            return mergePushSelected((MergeNode) qtn, toPushColumns);
        } else {
            if (toPushColumns.isEmpty()) {
                qtn.setUpRefers(isPushDownNode);
            } else if (qtn.isDistinct()) {
                List<Item> selList = qtn.getColumnsSelected();
                for (Item pdSel : toPushColumns) {
                    if (!selList.contains(pdSel)) {
                        selList.add(pdSel);
                    }
                }
                qtn.setUpRefers(isPushDownNode);
            } else {
                List<Item> selList = qtn.getColumnsSelected();
                selList.clear();
                boolean existSum = false;
                for (Item toPush : toPushColumns) {
                    selList.add(toPush);
                    existSum |= toPush.type().equals(Item.ItemType.SUM_FUNC_ITEM);
                }
                // @bug select sum(id) from (select id,sum(id) from t1) t
                // if only push id,it will miss sum(id)
                if (!existSum && qtn.getSumFuncs().size() > 0) {
                    selList.add(qtn.getSumFuncs().iterator().next());
                }
                qtn.setUpRefers(isPushDownNode);
            }
            PlanNode.PlanNodeType i = qtn.type();
            if (i == PlanNode.PlanNodeType.NONAME) {
                return qtn;
            } else if (i == PlanNode.PlanNodeType.TABLE) {
                return qtn;
            } else {
                for (PlanNode child : qtn.getChildren()) {
                    List<Item> referList = qtn.getColumnsReferedByChild(child);
                    if (referList.isEmpty()) {
                        referList.add(new ItemInt(1));
                    }
                    Collection<Item> pdRefers = getPushDownSel(qtn, child, referList);
                    pushSelected(child, pdRefers);
                }
                return qtn;
            }
        }
    }

    private static Collection<Item> getPushDownSel(PlanNode parent, PlanNode child, List<Item> selList) {
        // oldselectable->newselectbable
        HashMap<Item, Item> oldNewMap = new HashMap<>();
        HashMap<Item, Item> oldKeyKeyMap = new HashMap<>();
        for (Item sel : selList) {
            Item pdSel = oldNewMap.get(sel);
            if (pdSel == null) {
                pdSel = PlanUtil.pushDownItem(parent, sel);
                oldNewMap.put(sel, pdSel);
                oldKeyKeyMap.put(sel, sel);
            } else {
                Item sameKey = oldKeyKeyMap.get(sel);
                sel.setPushDownName(sameKey.getPushDownName());
            }
        }
        return oldNewMap.values();
    }

    // union's push is different , when toPushColumn.isEmpty,   merge's select can't be change
    private static PlanNode mergePushSelected(MergeNode merge, Collection<Item> toPushColumns) {
        if (toPushColumns.isEmpty() && merge.getOrderBys().isEmpty()) {
            for (PlanNode child : merge.getChildren()) {
                pushSelected(child, new HashSet<Item>());
            }
            return merge;
        }
        boolean canOverload = mergeNodeChildsCheck(merge) && !toPushColumns.isEmpty();
        final Map<String, Integer> colIndexs = merge.getColIndexs();
        List<Item> mergeSelects = null;
        if (toPushColumns.isEmpty()) {
            //  merge's select can't be change
            mergeSelects = new ArrayList<>();
            merge.setComeInFields(mergeSelects);
            mergeSelects.addAll(merge.getColumnsSelected());
        } else {
            mergeSelects = merge.getColumnsSelected();
        }
        if (canOverload) {
            mergeSelects.clear();
            mergeSelects.addAll(toPushColumns);
        } else {
            for (Item toPush : toPushColumns) {
                if (!mergeSelects.contains(toPush)) {
                    mergeSelects.add(toPush);
                }
            }
        }
        // add order by
        for (Order orderby : merge.getOrderBys()) {
            Item orderSel = orderby.getItem();
            mergePushOrderBy(orderSel, mergeSelects);
        }
        // push down the merge's select
        List<List<Item>> allChildPushs = new ArrayList<>(toPushColumns.size());
        for (Item toPush : mergeSelects) {
            // union's order by must be found in selects
            if (toPush.getPushDownName() == null && !toPush.type().equals(Item.ItemType.FIELD_ITEM))
                toPush.setPushDownName(toPush.getItemName());
            List<Item> childPushs = PlanUtil.getPushItemsToUnionChild(merge, toPush, colIndexs);
            allChildPushs.add(childPushs);
        }
        // make all child's count of pushing down is equal
        for (int index = 0; index < merge.getChildren().size(); index++) {
            List<Item> colSels = merge.getChildren().get(index).getColumnsSelected();
            colSels.clear();
            for (List<Item> childPushs : allChildPushs) {
                colSels.add(childPushs.get(index));
            }
            pushSelected(merge.getChildren().get(index), new HashSet<Item>());
        }
        return merge;
    }

    /**
     * check merge's subchild have distinct or aggregate function
     *
     * @param merge
     * @return
     */
    private static boolean mergeNodeChildsCheck(MergeNode merge) {
        for (PlanNode child : merge.getChildren()) {
            boolean cdis = child.isDistinct();
            boolean bsum = child.getSumFuncs().size() > 0;
            if (cdis || bsum)
                return false;
        }
        return true;
    }

    private static void mergePushOrderBy(Item orderSel, List<Item> mergeSelects) {
        if (orderSel instanceof ItemField) {
            if (!mergeSelects.contains(orderSel))
                mergeSelects.add(orderSel);
        } else if (orderSel instanceof ItemFunc) {
            ItemFunc func = (ItemFunc) orderSel;
            if (func.isWithSumFunc()) {
                for (int index = 0; index < func.getArgCount(); index++) {
                    Item arg = func.arguments().get(index);
                    mergePushOrderBy(arg, mergeSelects);
                }
            } else {
                if (!mergeSelects.contains(func)) {
                    mergeSelects.add(func);
                }
                // union's order by must be found from selects
                func.setPushDownName(func.getItemName());
            }
        } else if (orderSel instanceof ItemSum) {
            ItemSum func = (ItemSum) orderSel;
            for (int index = 0; index < func.getArgCount(); index++) {
                Item arg = func.arguments().get(index);
                mergePushOrderBy(arg, mergeSelects);
            }
        }
    }

}
