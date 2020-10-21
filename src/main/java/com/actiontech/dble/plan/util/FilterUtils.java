/*
 * Copyright (C) 2016-2020 ActionTech.
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
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.util.CollectionUtil;

import java.util.*;

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
            } else if (cond.functype() == Functype.COND_OR_FUNC) {

                //step1 divide the args into base  part
                Set<List<Item>> saveSet = new LinkedHashSet<>();
                for (int index = 0; index < cond.getArgCount(); index++) {
                    Item subFilter = cond.arguments().get(index);
                    if (subFilter == null)
                        continue;
                    List<Item> subSplits = splitFilter(subFilter);
                    saveSet.add(subSplits);
                }

                Map<String, List<Item>> itemMap = gourpByRefertTable(filter, saveSet);
                if (!CollectionUtil.isEmpty(itemMap)) {
                    for (Map.Entry<String, List<Item>> entry : itemMap.entrySet()) {
                        ItemCondOr x = new ItemCondOr(entry.getValue());
                        x.getReferTables().addAll(entry.getValue().get(0).getReferTables());
                        x.setWithUnValAble(true);
                        filterList.add(x);
                    }
                }
                filterList.add(cond);
            } else {
                filterList.add(cond);
            }
        } else {
            filterList.add(filter);
        }
        return filterList;
    }

    /**
     * group the or filter into Map<table,List<condition>>
     * try to optimize where with 'OR'
     * <p>
     * eg1: tabl1.join(table2).query((table1.id>5 && table1.name = table2.name)or (table2.id<10 && table1.name = table2.name)")
     * after optimized:
     * table1.query("table1.id>5").or(table2.query("table2.id<10").on("table1.name = table2.name")
     * <p>
     * eg2: tabl1.join(table2).query(table1.id = table2.id && (table1.name = a or table2.name = b))
     * after optimized:
     * table1.query("table1.name = a").or(table2.query("table2.name = b").on("table1.name = table2.name")
     *
     * @param self
     * @param saveSet
     * @return
     */
    public static Map<String, List<Item>> gourpByRefertTable(Item self, Set<List<Item>> saveSet) {
        Map<String, List<Item>> itemMap = new HashMap<>();

        //step2 turned to group the base filter by refertTable
        //only when a table has filter in every args of condition OR ,the table can be limit
        //if a refertTable meet the criteria ,the table and all it's filter will be
        jumpOut:
        for (PlanNode refertTable : self.getReferTables()) { // Traversing related tables
            if (refertTable instanceof TableNode) {
                String tableName = ((TableNode) refertTable).getTableName();

                //this loop is to check wether the table has filter in every Subconditions (....where subcondition1 or subcondition2 )
                for (List<Item> singleList : saveSet) {
                    boolean hasOutTable = false;
                    //in every subcondition the default relationship is 'AND'
                    //so the condition in subconditions also need be group by table
                    Map<String, List<Item>> itemMapForSingle = new HashMap<String, List<Item>>();
                    for (Item x : singleList) {
                        if (x.getReferTables().size() == 1) {
                            for (PlanNode backTable : x.getReferTables()) {
                                if (backTable instanceof TableNode) {
                                    if (((TableNode) backTable).getTableName().equals(tableName)) {
                                        hasOutTable = true;
                                        if (!itemMapForSingle.containsKey(tableName)) {
                                            itemMapForSingle.put(tableName, new ArrayList<Item>());
                                        }
                                        itemMapForSingle.get(tableName).add(x);
                                    }
                                }
                            }
                        }
                    }
                    if (!hasOutTable) {
                        //it the refertTable
                        itemMap.remove(tableName);
                        continue jumpOut;
                    }

                    //in every single list the sub expr  relationship default as 'AND'
                    //so just group the relationship in every sub expr
                    for (Map.Entry<String, List<Item>> entry : itemMapForSingle.entrySet()) {
                        if (!itemMap.containsKey(entry.getKey())) {
                            itemMap.put(tableName, new ArrayList<Item>());
                        }
                        if (entry.getValue().size() != 1) {
                            ItemCondAnd x = new ItemCondAnd(entry.getValue());
                            x.getReferTables().addAll(entry.getValue().get(0).getReferTables());
                            itemMap.get(tableName).add(x);
                        } else {
                            itemMap.get(tableName).add(entry.getValue().get(0));
                        }
                    }
                }
            }
        }
        return itemMap;
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
    public static ItemFuncEqual equal(Item column, Item value, int charsetIndex) {
        ItemFuncEqual f = new ItemFuncEqual(column, value, charsetIndex);
        PlanUtil.refreshReferTables(f);
        return f;
    }
}
