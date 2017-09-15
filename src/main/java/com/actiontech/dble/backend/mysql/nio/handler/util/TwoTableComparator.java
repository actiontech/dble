/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * the difference of the 2TableComparator and RowDataComparatoris that
 * the columns to be compared of join table are may different type.
 * eg: int compare with string
 *
 * @author ActionTech
 */
public class TwoTableComparator implements Comparator<RowDataPacket> {

    /* origin field, update before calculating the item */
    private List<Field> leftFields;
    private List<Field> rightFields;
    private List<ArgComparator> cmptors;
    private List<Boolean> ascs;

    public TwoTableComparator(List<FieldPacket> fps1, List<FieldPacket> fps2, List<Order> leftOrders,
                              List<Order> rightOrders, boolean isAllPushDown, DMLResponseHandler.HandlerType type) {
        this.leftFields = HandlerTool.createFields(fps1);
        this.rightFields = HandlerTool.createFields(fps2);
        ascs = new ArrayList<>();
        for (Order order : leftOrders) {
            ascs.add(order.getSortOrder() == SQLOrderingSpecification.ASC);
        }
        cmptors = new ArrayList<>();
        for (int index = 0; index < ascs.size(); index++) {
            Order leftOrder = leftOrders.get(index);
            Order rightOrder = rightOrders.get(index);
            Item leftCmpItem = HandlerTool.createItem(leftOrder.getItem(), leftFields, 0, isAllPushDown, type);
            Item rightCmpItem = HandlerTool.createItem(rightOrder.getItem(), rightFields, 0, isAllPushDown,
                    type);
            ArgComparator cmptor = new ArgComparator(leftCmpItem, rightCmpItem);
            cmptor.setCmpFunc(null, leftCmpItem, rightCmpItem, false);
            cmptors.add(cmptor);
        }
    }

    @Override
    public int compare(RowDataPacket o1, RowDataPacket o2) {
        if (ascs == null || ascs.size() == 0) // no join column, all same
            return 0;
        return compareRecursion(o1, o2, 0);
    }

    private int compareRecursion(RowDataPacket o1, RowDataPacket o2, int i) {
        HandlerTool.initFields(leftFields, o1.fieldValues);
        HandlerTool.initFields(rightFields, o2.fieldValues);
        ArgComparator cmptor = cmptors.get(i);
        boolean isAsc = ascs.get(i);
        int rs;
        if (isAsc) {
            rs = cmptor.compare();
        } else {
            rs = -1 * cmptor.compare();
        }
        if (rs != 0 || ascs.size() == (i + 1)) {
            return rs;
        } else {
            return compareRecursion(o1, o2, i + 1);
        }
    }
}
