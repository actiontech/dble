/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.row;

import com.actiontech.dble.sqlengine.mpp.ColMeta;
import com.actiontech.dble.sqlengine.mpp.OrderCol;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Created by zagnix on 2016/6/6.
 */
public class StructType {

    private final Map<String, ColMeta> columToIndx;
    private final int fieldCount;

    private OrderCol[] orderCols = null;

    public StructType(@Nonnull Map<String, ColMeta> columToIndx, int fieldCount) {
        assert fieldCount >= 0;
        this.columToIndx = columToIndx;
        this.fieldCount = fieldCount;
    }

    public int length() {
        return fieldCount;
    }

    public Map<String, ColMeta> getColumToIndx() {
        return columToIndx;
    }

    public OrderCol[] getOrderCols() {
        return orderCols;
    }

    public void setOrderCols(OrderCol[] orderCols) {
        this.orderCols = orderCols;
    }

    public long apply(int i) {
        return 0;
    }
}
