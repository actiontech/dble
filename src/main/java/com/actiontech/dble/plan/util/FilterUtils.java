/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.ItemFunc.Functype;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCond;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;

import java.util.ArrayList;
import java.util.List;

public final class FilterUtils {
    private FilterUtils() {
    }

    /**
     * split the filter until filter is 'boolean filer' or 'orfilter'
     *
     * @param filter
     * @return
     */
    public static List<Item> splitFilter(Item filter) {
        if (filter == null)
            throw new RuntimeException("check filter before split");
        List<Item> filterList = new ArrayList<>();
        if (filter.type() == Item.ItemType.COND_ITEM) {
            ItemCond cond = (ItemCond) filter;
            if (cond.functype() == Functype.COND_AND_FUNC) {
                for (int index = 0; index < cond.getArgCount(); index++) {
                    Item subFilter = cond.arguments().get(index);
                    if (subFilter == null)
                        continue;
                    List<Item> subSplits = splitFilter(subFilter);
                    filterList.addAll(subSplits);
                }
            } else {
                filterList.add(cond);
            }
        } else {
            filterList.add(filter);
        }
        return filterList;
    }

    public static ItemCond createLogicalFilterNoName(boolean and, List<Item> subFilters) {
        ItemCond cond = null;
        if (and)
            cond = new ItemCondAnd(subFilters);
        else
            cond = new ItemCondOr(subFilters);
        PlanUtil.refreshReferTables(cond);
        return cond;
    }

    public static Item and(List<Item> filters) {
        if (filters == null || filters.isEmpty())
            return null;
        List<Item> subFilters = new ArrayList<>();
        for (Item filter : filters) {
            if (filter == null)
                continue;
            if (filter.type() == Item.ItemType.COND_ITEM && ((ItemCond) filter).functype() == Functype.COND_AND_FUNC) {
                subFilters.addAll(filter.arguments());
            } else
                subFilters.add(filter);
        }
        if (subFilters.isEmpty())
            return null;
        else if (subFilters.size() == 1)
            return subFilters.get(0);
        else {
            return createLogicalFilterNoName(true, subFilters);
        }
    }

    /**
     * create and condition
     */
    public static Item and(Item root, Item o) {
        List<Item> list = new ArrayList<>();
        list.add(root);
        list.add(o);
        return and(list);
    }

    public static Item or(List<Item> filters) {
        if (filters == null)
            return null;
        List<Item> subFilters = new ArrayList<>();
        for (Item filter : filters) {
            if (filter == null)
                continue;
            if (filter.type() == Item.ItemType.COND_ITEM && ((ItemCond) filter).functype() == Functype.COND_OR_FUNC) {
                subFilters.addAll(filter.arguments());
            } else
                subFilters.add(filter);
        }
        if (subFilters.isEmpty())
            return new ItemInt(0);
        else if (subFilters.size() == 1)
            return subFilters.get(0);
        else {
            return createLogicalFilterNoName(false, subFilters);
        }
    }

    /**
     * create or condition
     */
    public static Item or(Item root, Item o) {
        List<Item> list = new ArrayList<>();
        list.add(root);
        list.add(o);
        return or(list);
    }

    /**
     * create equal filter
     */
    public static ItemFuncEqual equal(Item column, Item value) {
        ItemFuncEqual f = new ItemFuncEqual(column, value);
        PlanUtil.refreshReferTables(f);
        return f;
    }

    public static ItemFuncGt greaterThan(Item column, Item value) {
        ItemFuncGt f = new ItemFuncGt(column, value);
        PlanUtil.refreshReferTables(f);
        return f;
    }

    public static ItemFuncLt lessThan(Item column, Item value) {
        ItemFuncLt f = new ItemFuncLt(column, value);
        PlanUtil.refreshReferTables(f);
        return f;
    }

    public static ItemFuncGe greaterEqual(Item column, Item value) {
        ItemFuncGe f = new ItemFuncGe(column, value);
        PlanUtil.refreshReferTables(f);
        return f;
    }

    public static ItemFuncLe lessEqual(Item column, Item value) {
        ItemFuncLe f = new ItemFuncLe(column, value);
        PlanUtil.refreshReferTables(f);
        return f;
    }

    public static ItemFuncNe notEqual(Item column, Item value) {
        ItemFuncNe f = new ItemFuncNe(column, value);
        PlanUtil.refreshReferTables(f);
        return f;
    }
}
