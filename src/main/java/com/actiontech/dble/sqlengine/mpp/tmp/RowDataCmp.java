/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp.tmp;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.sqlengine.mpp.OrderCol;
import com.actiontech.dble.sqlengine.mpp.RowDataPacketSorter;

import java.util.Comparator;

/**
 * @author coderczp-2014-12-8
 */
public class RowDataCmp implements Comparator<RowDataPacket> {

    private OrderCol[] orderCols;

    public RowDataCmp(OrderCol[] orderCols) {
        this.orderCols = orderCols;
    }

    @Override
    public int compare(RowDataPacket o1, RowDataPacket o2) {
        OrderCol[] tmp = this.orderCols;
        int cmp = 0;
        //compare the columns of order by
        int type = OrderCol.COL_ORDER_TYPE_ASC;
        for (OrderCol aTmp : tmp) {
            int colIndex = aTmp.colMeta.getColIndex();
            byte[] left = o1.fieldValues.get(colIndex);
            byte[] right = o2.fieldValues.get(colIndex);
            if (aTmp.orderType == type) {
                cmp = RowDataPacketSorter.compareObject(left, right, aTmp);
            } else {
                cmp = RowDataPacketSorter.compareObject(right, left, aTmp);
            }
            if (cmp != 0) {
                return cmp;
            }
        }
        return cmp;
    }

}
