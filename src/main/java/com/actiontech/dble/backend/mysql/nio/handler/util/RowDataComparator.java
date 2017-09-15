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
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RowDataComparator implements Comparator<RowDataPacket> {

    private List<Field> sourceFields;
    private List<Item> cmpItems;

    private List<Field> cmpFields;
    private List<Boolean> ascs;


    public RowDataComparator(List<FieldPacket> fps, List<Order> orders, boolean allPushDown, DMLResponseHandler.HandlerType type) {
        sourceFields = HandlerTool.createFields(fps);
        if (orders != null && orders.size() > 0) {
            ascs = new ArrayList<>();
            cmpFields = new ArrayList<>();
            cmpItems = new ArrayList<>();
            for (Order order : orders) {
                Item cmpItem = HandlerTool.createItem(order.getItem(), sourceFields, 0, allPushDown, type);
                cmpItems.add(cmpItem);
                FieldPacket tmpFp = new FieldPacket();
                cmpItem.makeField(tmpFp);
                Field cmpField = HandlerTool.createField(tmpFp);
                cmpFields.add(cmpField);
                ascs.add(order.getSortOrder() == SQLOrderingSpecification.ASC);
            }
        }
    }


    public void sort(List<RowDataPacket> rows) {
        Comparator<RowDataPacket> c = new Comparator<RowDataPacket>() {

            @Override
            public int compare(RowDataPacket o1, RowDataPacket o2) {
                if (RowDataComparator.this.ascs != null && RowDataComparator.this.ascs.size() > 0)
                    return RowDataComparator.this.compare(o1, o2);
                else {
                    return -1;
                }
            }
        };
        Collections.sort(rows, c);
    }

    @Override
    public int compare(RowDataPacket o1, RowDataPacket o2) {
        if (this.ascs != null && this.ascs.size() > 0) {
            int cmpValue = cmp(o1, o2, 0);
            return cmpValue;
        } else {
            return 0;
        }
    }

    private List<byte[]> getCmpBytes(RowDataPacket o) {
        if (o.getCmpValue() == null) {
            HandlerTool.initFields(sourceFields, o.fieldValues);
            List<byte[]> bo = HandlerTool.getItemListBytes(cmpItems);
            o.setCmpValue(bo);
        }
        return o.getCmpValue();
    }

    private int cmp(RowDataPacket o1, RowDataPacket o2, int index) {
        List<byte[]> bo1 = getCmpBytes(o1);
        List<byte[]> bo2 = getCmpBytes(o2);
        boolean isAsc = ascs.get(index);
        Field field = cmpFields.get(index);
        byte[] b1 = bo1.get(index);
        byte[] b2 = bo2.get(index);
        int rs;
        if (isAsc) {
            rs = field.compare(b1, b2);
        } else {
            rs = field.compare(b2, b1);
        }
        if (rs != 0 || cmpFields.size() == (index + 1)) {
            return rs;
        } else {
            return cmp(o1, o2, index + 1);
        }
    }

}
