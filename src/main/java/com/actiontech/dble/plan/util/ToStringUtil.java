/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.item.Item;

import java.util.List;

public final class ToStringUtil {
    private ToStringUtil() {
    }

    /**
     * add tab like <[ ]>
     *
     * @param count
     * @return
     */
    public static String getTab(int count) {
        StringBuilder tab = new StringBuilder();
        for (int i = 0; i < count; i++)
            tab.append("    ");
        return tab.toString();
    }

    /**
     * concat \n
     */
    public static void appendln(StringBuilder sb, String v) {
        sb.append(v).append("\n");
    }

    /**
     * get the string of itemlist
     *
     * @param itemList
     * @return
     */
    public static String itemString(Item item) {
        if (item == null)
            return "null";
        return item.toString();
    }

    /**
     * get the string of itemlist
     *
     * @param itemList
     * @return
     */
    public static String itemListString(List itemList) {
        if (itemList == null)
            return "null";
        if (itemList.isEmpty())
            return " ";
        boolean isFirst = true;
        StringBuilder sb = new StringBuilder();
        for (Object item : itemList) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }
            sb.append(item);
        }
        return sb.toString();
    }

    public static String orderString(Order order) {
        if (order == null)
            return "null";
        return order.getItem() + " " + order.getSortOrder();
    }

    public static String orderListString(List<Order> orderList) {
        if (orderList == null)
            return "null";
        if (orderList.isEmpty())
            return " ";
        boolean isFirst = true;
        StringBuilder sb = new StringBuilder();
        for (Order order : orderList) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }
            sb.append(orderString(order));
        }
        return sb.toString();
    }
}
