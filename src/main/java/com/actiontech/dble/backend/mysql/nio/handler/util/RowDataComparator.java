/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.field.num.FieldLong;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.subquery.ItemScalarSubQuery;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RowDataComparator implements Comparator<RowDataPacket> {

    private List<Field> sourceFields;
    private List<Item> cmpItems;

    private List<Field> cmpFields;
    private List<Boolean> ascList;


    public RowDataComparator(List<FieldPacket> fps, List<Order> orders, boolean allPushDown, DMLResponseHandler.HandlerType type) {
        sourceFields = HandlerTool.createFields(fps);
        if (orders != null && orders.size() > 0) {
            ascList = new ArrayList<>();
            cmpFields = new ArrayList<>();
            cmpItems = new ArrayList<>();
            for (Order order : orders) {
                if (order.getItem() instanceof ItemScalarSubQuery) {
                    if (((ItemScalarSubQuery) order.getItem()).getValue() == null) {
                        continue;
                    }
                }
                Item cmpItem = createOrderItem(order.getItem(), this.sourceFields, allPushDown, type);
                if (cmpItem == null) {
                    continue;
                }
                cmpItems.add(cmpItem);
                FieldPacket tmpFp = new FieldPacket();
                cmpItem.makeField(tmpFp);
                Field cmpField = HandlerTool.createField(tmpFp);
                cmpFields.add(cmpField);
                ascList.add(order.getSortOrder() == SQLOrderingSpecification.ASC);
            }
        }
    }

    public RowDataComparator(List<Field> fields, List<Order> orders) {
        sourceFields = fields;
        if (orders != null && orders.size() > 0) {
            ascList = new ArrayList<>();
            cmpFields = new ArrayList<>();
            cmpItems = new ArrayList<>();
            for (Order order : orders) {
                Item cmpItem = HandlerTool.createFieldItem(order.getItem(), sourceFields, 0);
                cmpItems.add(cmpItem);
                FieldPacket tmpFp = new FieldPacket();
                cmpItem.makeField(tmpFp);
                Field cmpField = HandlerTool.createField(tmpFp);
                cmpFields.add(cmpField);
                ascList.add(order.getSortOrder() == SQLOrderingSpecification.ASC);
            }
        }
    }
    private static Item createOrderItem(Item sel, List<Field> fields, boolean allPushDown, DMLResponseHandler.HandlerType type) {
        Item ret = null;
        if (sel.basicConstItem())
            return sel;
        if (sel instanceof ItemScalarSubQuery) {
            ItemScalarSubQuery scalarSubQuery = (ItemScalarSubQuery) sel;
            if ((scalarSubQuery.getValue() instanceof ItemField) &&
                    (((ItemField) scalarSubQuery.getValue()).getField() instanceof FieldLong)) {
                FieldLong returnValue = (FieldLong) ((ItemField) scalarSubQuery.getValue()).getField();
                int fieldIndex = (int) returnValue.valDecimal().longValue() - 1;
                ret = new ItemField(fields.get(fieldIndex));
            }
            // in mysql even the subquery return a field name , the order would be ignored
            return ret;
        }
        ret = HandlerTool.createItem(sel, fields, 0, allPushDown, type);
        return ret;
    }


    public void sort(List<RowDataPacket> rows) {
        Comparator<RowDataPacket> c = new Comparator<RowDataPacket>() {

            @Override
            public int compare(RowDataPacket o1, RowDataPacket o2) {
                if (RowDataComparator.this.ascList != null && RowDataComparator.this.ascList.size() > 0)
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
        if (this.ascList != null && this.ascList.size() > 0) {
            return cmp(o1, o2, 0);
        } else {
            return 0;
        }
    }

    private List<byte[]> getCmpBytes(RowDataPacket o) {
        if (o.getCmpValue(this) == null) {
            HandlerTool.initFields(sourceFields, o.fieldValues);
            List<byte[]> bo = HandlerTool.getItemListBytes(cmpItems);
            o.cacheCmpValue(this, bo);
        }
        return o.getCmpValue(this);
    }

    private int cmp(RowDataPacket o1, RowDataPacket o2, int index) {
        List<byte[]> bo1 = getCmpBytes(o1);
        List<byte[]> bo2 = getCmpBytes(o2);
        boolean isAsc = ascList.get(index);
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
