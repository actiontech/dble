/**
 *
 */
package io.mycat.plan;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import io.mycat.plan.common.item.Item;

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

}
