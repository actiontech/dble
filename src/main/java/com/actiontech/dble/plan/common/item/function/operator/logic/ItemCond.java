/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.logic;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemBoolFunc;

import java.util.ArrayList;
import java.util.List;


public abstract class ItemCond extends ItemBoolFunc {
    protected boolean abortOnNull = true;
    List<Item> list;

    public ItemCond(List<Item> args) {
        super(args);
        list = new ArrayList<>();
        list.addAll(args);
    }

    public void add(Item item) {
        list.add(item);
    }

    public void addAtHead(Item item) {
        list.add(0, item);
    }

    public void addAtHead(List<Item> itemList) {
        list.addAll(0, itemList);
    }

    @Override
    public ItemType type() {
        return ItemType.COND_ITEM;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_LONGLONG;
    }

}
