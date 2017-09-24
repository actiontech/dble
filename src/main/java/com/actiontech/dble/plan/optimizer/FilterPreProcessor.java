/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCond;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;

import java.util.*;

/**
 * http://dev.mysql.com/doc/refman/5.7/en/where-optimizations.html
 *
 * @author ActionTech
 * @CreateTime Mar 16, 2016
 */
public final class FilterPreProcessor {
    private FilterPreProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        MergeHavingFilter.optimize(qtn);
        qtn = preProcess(qtn);
        return qtn;
    }


    private static PlanNode preProcess(PlanNode qtn) {
        qtn.having(processFilter(qtn.getHavingFilter()));
        qtn.query(processFilter(qtn.getWhereFilter()));
        if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            for (int i = 0; i < ((JoinNode) qtn).getJoinFilter().size(); i++) {
                processFilter(jn.getJoinFilter().get(i));
            }
            jn.setOtherJoinOnFilter(processFilter(jn.getOtherJoinOnFilter()));
        }
        for (PlanNode child : qtn.getChildren()) {
            preProcess(child);
        }
        return qtn;
    }

    private static Item processFilter(Item root) {
        if (root == null) {
            return null;
        }

        root = shortestFilter(root);
        root = processOneFilter(root);
        root = convertOrToIn(root);
        return root;
    }

    /**
     * optimizer 0=1/1=1/true
     */
    private static Item shortestFilter(Item root) {
        if (root == null)
            return root;
        if (root.canValued()) {
            boolean value = root.valBool();
            if (value)
                return new ItemInt(1);
            else
                return new ItemInt(0);
        } else if (root.type().equals(Item.ItemType.COND_ITEM)) {
            ItemCond cond = (ItemCond) root;
            for (int index = 0; index < cond.getArgCount(); index++) {
                Item shortedsub = shortestFilter(cond.arguments().get(index));
                cond.arguments().set(index, shortedsub);
            }
            boolean isAnd = cond.functype().equals(ItemFunc.Functype.COND_AND_FUNC);
            List<Item> newSubFilters = new ArrayList<>();
            for (Item sub : cond.arguments()) {
                if (sub == null)
                    continue;
                if (sub.canValued()) {
                    boolean value = sub.valBool();
                    if (value && !isAnd)
                        return new ItemInt(1);
                    if (!value && isAnd)
                        return new ItemInt(0);
                } else {
                    newSubFilters.add(sub);
                }
            }
            if (isAnd)
                return FilterUtils.and(newSubFilters);
            else
                return FilterUtils.or(newSubFilters);
        } else {
            return root;
        }
    }

    /**
     * change "const op column" to "column op const"
     *
     * @param root
     * @return
     */
    private static Item processOneFilter(Item root) {
        if (root == null) {
            return null;
        }
        Item newRoot = root;
        if (root instanceof ItemBoolFunc2) {
            Item a = root.arguments().get(0);
            Item b = root.arguments().get(1);
            if (a.basicConstItem() && !b.basicConstItem()) {
                if (root instanceof ItemFuncGe) {
                    newRoot = new ItemFuncLe(b, a);
                } else if (root instanceof ItemFuncGt) {
                    newRoot = new ItemFuncLt(b, a);
                } else if (root instanceof ItemFuncLt) {
                    newRoot = new ItemFuncGt(b, a);
                } else if (root instanceof ItemFuncLe) {
                    newRoot = new ItemFuncGe(b, a);
                } else {
                    root.arguments().set(1, a);
                    root.arguments().set(0, b);
                    root.setItemName(null);
                }
                newRoot.getReferTables().addAll(root.getReferTables());
            }
        } else if (root instanceof ItemCond) {
            ItemCond condfun = (ItemCond) root;
            List<Item> newArgs = new ArrayList<>();
            for (Item arg : condfun.arguments()) {
                Item newArg = processOneFilter(arg);
                if (newArg != null)
                    newArgs.add(newArg);
            }
            if (condfun.functype().equals(ItemFunc.Functype.COND_AND_FUNC))
                newRoot = FilterUtils.and(newArgs);
            else
                newRoot = FilterUtils.or(newArgs);
        }
        return newRoot;
    }

    /**
     * change single Logicalfilter(or) into in
     *
     * @param filter
     */
    private static Item convertOrToIn(Item filter) {
        if (filter == null)
            return null;
        if (filter.type().equals(Item.ItemType.COND_ITEM)) {
            if (filter instanceof ItemCondAnd) {
                ItemCondAnd andFilter = (ItemCondAnd) filter;
                for (int index = 0; index < andFilter.getArgCount(); index++) {
                    andFilter.arguments().set(index, convertOrToIn(andFilter.arguments().get(index)));
                }
                andFilter.setItemName(null);
                PlanUtil.refreshReferTables(andFilter);
                return andFilter;
            } else {
                // or
                ItemCondOr orFilter = (ItemCondOr) filter;
                HashMap<Item, Set<Item>> inMap = new HashMap<>();
                List<Item> newSubFilterList = new ArrayList<>();
                for (int index = 0; index < orFilter.getArgCount(); index++) {
                    Item subFilter = orFilter.arguments().get(index);
                    if (subFilter == null)
                        continue;
                    if (subFilter instanceof ItemFuncEqual) {
                        Item a = subFilter.arguments().get(0);
                        Item b = subFilter.arguments().get(1);
                        if (!a.canValued() && b.canValued()) {
                            if (!inMap.containsKey(a))
                                inMap.put(a, new HashSet<Item>());
                            inMap.get(a).add(b);
                        }
                    } else {
                        Item subNew = convertOrToIn(subFilter);
                        newSubFilterList.add(subNew);
                    }
                }
                for (Map.Entry<Item, Set<Item>> entry : inMap.entrySet()) {
                    List<Item> args = new ArrayList<>();
                    args.add(entry.getKey());
                    args.addAll(entry.getValue());
                    ItemFuncIn inItem = new ItemFuncIn(args, false);
                    PlanUtil.refreshReferTables(inItem);
                    newSubFilterList.add(inItem);
                }
                return FilterUtils.or(newSubFilterList);
            }
        }
        return filter;
    }

}
