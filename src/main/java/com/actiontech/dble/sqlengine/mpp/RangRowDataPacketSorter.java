/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.sqlengine.mpp.tmp.RowDataSorter;


public class RangRowDataPacketSorter extends RowDataSorter {
    public RangRowDataPacketSorter(OrderCol[] orderCols) {
        super(orderCols);
    }

    public boolean ascDesc(int byColumnIndex) {
        return this.orderCols[byColumnIndex].orderType == OrderCol.COL_ORDER_TYPE_ASC;
    }

    public int compareRowData(RowDataPacket l, RowDataPacket r, int byColumnIndex) {
        byte[] left = l.fieldValues.get(this.orderCols[byColumnIndex].colMeta.getColIndex());
        byte[] right = r.fieldValues.get(this.orderCols[byColumnIndex].colMeta.getColIndex());

        return RowDataPacketSorter.compareObject(left, right, this.orderCols[byColumnIndex]);
    }
}
