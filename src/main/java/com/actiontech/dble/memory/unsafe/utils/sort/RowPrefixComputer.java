/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.utils.sort;


import com.actiontech.dble.memory.unsafe.row.StructType;
import com.actiontech.dble.memory.unsafe.row.UnsafeRow;
import com.actiontech.dble.memory.unsafe.utils.BytesTools;
import com.actiontech.dble.sqlengine.mpp.ColMeta;
import com.actiontech.dble.sqlengine.mpp.OrderCol;

import java.io.UnsupportedEncodingException;

/**
 * Created by zagnix on 2016/6/20.
 */
public class RowPrefixComputer extends UnsafeExternalRowSorter.PrefixComputer {
    private final ColMeta colMeta;

    public RowPrefixComputer(StructType schema) {
        /**
         * get the index of the first key word of order
         */
        int orderIndex = 0;
        OrderCol[] orderCols = schema.getOrderCols();

        if (orderCols != null) {
            for (int i = 0; i < orderCols.length; i++) {
                ColMeta meta = orderCols[i].colMeta;
                if (meta.getColIndex() == 0) {
                    orderIndex = i;
                    break;
                }
            }

            this.colMeta = orderCols[orderIndex].colMeta;
        } else {
            this.colMeta = null;
        }
    }

    protected long computePrefix(UnsafeRow row) throws UnsupportedEncodingException {

        if (this.colMeta == null) {
            return 0;
        }

        int orderIndexType = colMeta.getColType();

        byte[] rowIndexElem = null;

        if (!row.isNullAt(colMeta.getColIndex())) {
            rowIndexElem = row.getBinary(colMeta.getColIndex());

            /**
             * the first order by column
             */
            switch (orderIndexType) {
                case ColMeta.COL_TYPE_INT:
                case ColMeta.COL_TYPE_LONG:
                case ColMeta.COL_TYPE_INT24:
                    return BytesTools.getInt(rowIndexElem);
                case ColMeta.COL_TYPE_SHORT:
                    return BytesTools.getShort(rowIndexElem);
                case ColMeta.COL_TYPE_LONGLONG:
                    return BytesTools.getLong(rowIndexElem);
                case ColMeta.COL_TYPE_FLOAT:
                    return PrefixComparators.DoublePrefixComparator.computePrefix(BytesTools.getFloat(rowIndexElem));
                case ColMeta.COL_TYPE_DOUBLE:
                case ColMeta.COL_TYPE_DECIMAL:
                case ColMeta.COL_TYPE_NEWDECIMAL:
                    return PrefixComparators.DoublePrefixComparator.computePrefix(BytesTools.getDouble(rowIndexElem));
                case ColMeta.COL_TYPE_DATE:
                case ColMeta.COL_TYPE_TIMSTAMP:
                case ColMeta.COL_TYPE_TIME:
                case ColMeta.COL_TYPE_YEAR:
                case ColMeta.COL_TYPE_DATETIME:
                case ColMeta.COL_TYPE_NEWDATE:
                case ColMeta.COL_TYPE_BIT:
                case ColMeta.COL_TYPE_VAR_STRING:
                case ColMeta.COL_TYPE_STRING:
                    // ENUM and SET ar all string
                case ColMeta.COL_TYPE_ENUM:
                case ColMeta.COL_TYPE_SET:
                    return PrefixComparators.BinaryPrefixComparator.computePrefix(rowIndexElem);
                // not support BLOB,GEOMETRY
                default:
                    break;
            }
            return 0;
        } else {
            rowIndexElem = new byte[1];
            rowIndexElem[0] = UnsafeRow.NULL_MARK;
            return PrefixComparators.BinaryPrefixComparator.computePrefix(rowIndexElem);
        }
    }
}
