package io.mycat.plan.util;

import io.mycat.plan.Order;
import io.mycat.plan.common.item.Item;

import java.util.List;

public class ToStringUtil {
    /**
     * add tab like <[ ]>
     *
     * @param count
     * @return
     */
    public static String getTab(int count) {
        StringBuffer tab = new StringBuffer();
        for (int i = 0; i < count; i++)
            tab.append("    ");
        return tab.toString();
    }

    /**
     * 拼接换行符
     */
    public static void appendln(StringBuilder sb, String v) {
        sb.append(v).append("\n");
    }

    /**
     * 获取itemlist的string表示
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
     * 获取itemlist的string表示
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
