/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

public class OrderCol {
    public final int orderType;
    public final ColMeta colMeta;

    public static final int COL_ORDER_TYPE_ASC = 0; // ASC
    public static final int COL_ORDER_TYPE_DESC = 1; // DESC

    public OrderCol(ColMeta colMeta, int orderType) {
        super();
        this.colMeta = colMeta;
        this.orderType = orderType;
    }

    public int getOrderType() {
        return orderType;
    }

    public ColMeta getColMeta() {
        return colMeta;
    }


}
