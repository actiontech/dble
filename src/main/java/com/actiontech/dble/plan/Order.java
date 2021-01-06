/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan;

import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

public class Order {
    private Item item;
    private SQLOrderingSpecification sortOrder;

    public Order(Item item) {
        this(item, SQLOrderingSpecification.ASC);
    }

    public Order(Item item, SQLOrderingSpecification sortOrder) {
        this.item = item;
        this.sortOrder = sortOrder;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public SQLOrderingSpecification getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SQLOrderingSpecification sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Override
    public String toString() {
        return "order by " + item.toString() + " " + sortOrder;
    }

    public Order copy() {
        return new Order(item.cloneStruct(), sortOrder);
    }

    @Override
    public int hashCode() {
        return sortOrder.name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Order) {
            Order order = (Order) obj;
            return this.getItem().equals(order.getItem()) && this.getSortOrder().equals(order.getSortOrder());
        }
        return false;
    }
}
