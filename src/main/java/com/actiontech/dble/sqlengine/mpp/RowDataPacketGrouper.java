/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.ByteUtil;
import com.actiontech.dble.util.CompareUtil;
import com.actiontech.dble.util.LongUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * implement group function select a,count(*),sum(*) from A group by a
 *
 * @author wuzhih
 */
public class RowDataPacketGrouper {

    private List<RowDataPacket> result = Collections.synchronizedList(new ArrayList<RowDataPacket>());
    private final MergeCol[] mergCols;
    private final int[] groupColumnIndexs;
    private boolean isMergAvg = false;
    private HavingCols havingCols;

    public RowDataPacketGrouper(int[] groupColumnIndexs, MergeCol[] mergCols, HavingCols havingCols) {
        super();
        this.groupColumnIndexs = groupColumnIndexs;
        this.mergCols = mergCols;
        this.havingCols = havingCols;
    }

    public List<RowDataPacket> getResult() {
        if (!isMergAvg) {
            for (RowDataPacket row : result) {
                mergAvg(row);
            }
            isMergAvg = true;
        }

        if (havingCols != null) {
            filterHaving();
        }

        return result;
    }

    private void filterHaving() {
        if (havingCols.getColMeta() == null || result == null) {
            return;
        }
        Iterator<RowDataPacket> it = result.iterator();
        byte[] right = havingCols.getRight().getBytes(
                StandardCharsets.UTF_8);
        int index = havingCols.getColMeta().getColIndex();
        int colType = havingCols.getColMeta().getColType();    // Added by winbill. 20160312.
        while (it.hasNext()) {
            RowDataPacket rowDataPacket = it.next();
            switch (havingCols.getOperator()) {
                case "=":
                    /* Add parameter of colType, Modified by winbill. 20160312. */
                    if (eq(rowDataPacket.fieldValues.get(index), right, colType)) {
                        it.remove();
                    }
                    break;
                case ">":
                    /* Add parameter of colType, Modified by winbill. 20160312. */
                    if (gt(rowDataPacket.fieldValues.get(index), right, colType)) {
                        it.remove();
                    }
                    break;
                case "<":
                    /* Add parameter of colType, Modified by winbill. 20160312. */
                    if (lt(rowDataPacket.fieldValues.get(index), right, colType)) {
                        it.remove();
                    }
                    break;
                case ">=":
                    /* Add parameter of colType, Modified by winbill. 20160312. */
                    if (gt(rowDataPacket.fieldValues.get(index), right, colType) && eq(rowDataPacket.fieldValues.get(index), right, colType)) {
                        it.remove();
                    }
                    break;
                case "<=":
                    /* Add parameter of colType, Modified by winbill. 20160312. */
                    if (lt(rowDataPacket.fieldValues.get(index), right, colType) && eq(rowDataPacket.fieldValues.get(index), right, colType)) {
                        it.remove();
                    }
                    break;
                case "!=":
                    /* Add parameter of colType, Modified by winbill. 20160312. */
                    if (neq(rowDataPacket.fieldValues.get(index), right, colType)) {
                        it.remove();
                    }
                    break;
                default:
                    break;
            }
        }

    }

    /*
     * Using new compare function instead of compareNumberByte
     * Modified by winbill. 20160312.
     */
    private boolean lt(byte[] l, byte[] r, final int colType) {
        return -1 != RowDataPacketGrouper.compareObject(l, r, colType);
    }

    private boolean gt(byte[] l, byte[] r, final int colType) {
        return 1 != RowDataPacketGrouper.compareObject(l, r, colType);
    }

    private boolean eq(byte[] l, byte[] r, final int colType) {
        return 0 != RowDataPacketGrouper.compareObject(l, r, colType);
    }

    private boolean neq(byte[] l, byte[] r, final int colType) {
        return 0 == RowDataPacketGrouper.compareObject(l, r, colType);
    }

    /*
     * Compare with the value of having column
     * winbill. 20160312.
     */
    public static int compareObject(byte[] left, byte[] right, final int colType) {
        switch (colType) {
            case ColMeta.COL_TYPE_SHORT:
            case ColMeta.COL_TYPE_INT:
            case ColMeta.COL_TYPE_INT24:
            case ColMeta.COL_TYPE_LONG:
                return CompareUtil.compareInt(ByteUtil.getInt(left), ByteUtil.getInt(right));
            case ColMeta.COL_TYPE_LONGLONG:
                return CompareUtil.compareLong(ByteUtil.getLong(left), ByteUtil.getLong(right));
            case ColMeta.COL_TYPE_FLOAT:
            case ColMeta.COL_TYPE_DOUBLE:
            case ColMeta.COL_TYPE_DECIMAL:
            case ColMeta.COL_TYPE_NEWDECIMAL:
                return CompareUtil.compareDouble(ByteUtil.getDouble(left), ByteUtil.getDouble(right));
            case ColMeta.COL_TYPE_DATE:
            case ColMeta.COL_TYPE_TIMSTAMP:
            case ColMeta.COL_TYPE_TIME:
            case ColMeta.COL_TYPE_YEAR:
            case ColMeta.COL_TYPE_DATETIME:
            case ColMeta.COL_TYPE_NEWDATE:
            case ColMeta.COL_TYPE_BIT:
            case ColMeta.COL_TYPE_VAR_STRING:
            case ColMeta.COL_TYPE_STRING:
                // ENUM and SET are String
            case ColMeta.COL_TYPE_ENUM:
            case ColMeta.COL_TYPE_SET:
                return ByteUtil.compareNumberByte(left, right);
            // not support BLOB and GEOMETRY
            default:
                break;
        }
        return 0;
    }

    public void addRow(RowDataPacket rowDataPkg) {
        for (RowDataPacket row : result) {
            if (sameGropuColums(rowDataPkg, row)) {
                aggregateRow(row, rowDataPkg);
                return;
            }
        }

        // not aggreated ,insert new
        result.add(rowDataPkg);

    }

    private void aggregateRow(RowDataPacket toRow, RowDataPacket newRow) {
        if (mergCols == null) {
            return;
        }
        for (MergeCol merg : mergCols) {
            if (merg.mergeType != MergeCol.MERGE_AVG) {
                byte[] mergeValue = mertFields(
                        toRow.fieldValues.get(merg.colMeta.getColIndex()),
                        newRow.fieldValues.get(merg.colMeta.getColIndex()),
                        merg.colMeta.getColType(), merg.mergeType);
                if (mergeValue != null) {
                    toRow.fieldValues.set(merg.colMeta.getColIndex(), mergeValue);
                }
            }
        }


    }

    private void mergAvg(RowDataPacket toRow) {
        if (mergCols == null) {
            return;
        }

        Set<Integer> rmIndexSet = new HashSet<>();
        for (MergeCol merg : mergCols) {
            if (merg.mergeType == MergeCol.MERGE_AVG) {
                byte[] mergeValue = mertFields(
                        toRow.fieldValues.get(merg.colMeta.getAvgSumIndex()),
                        toRow.fieldValues.get(merg.colMeta.getAvgCountIndex()),
                        merg.colMeta.getColType(), merg.mergeType);
                if (mergeValue != null) {
                    toRow.fieldValues.set(merg.colMeta.getAvgSumIndex(), mergeValue);
                    rmIndexSet.add(merg.colMeta.getAvgCountIndex());
                }
            }
        }
        for (Integer index : rmIndexSet) {
            toRow.fieldValues.remove(index);
            toRow.setFieldCount(toRow.getFieldCount() - 1);
        }


    }

    private byte[] mertFields(byte[] bs, byte[] bs2, int colType, int mergeType) {
        // System.out.println("mergeType:"+ mergeType+" colType "+colType+
        // " field:"+Arrays.toString(bs)+ " ->  "+Arrays.toString(bs2));
        if (bs2 == null || bs2.length == 0) {
            return bs;
        } else if (bs == null || bs.length == 0) {
            return bs2;
        }
        switch (mergeType) {
            case MergeCol.MERGE_SUM:
                if (colType == ColMeta.COL_TYPE_DOUBLE || colType == ColMeta.COL_TYPE_FLOAT) {

                    Double vale = ByteUtil.getDouble(bs) + ByteUtil.getDouble(bs2);
                    return vale.toString().getBytes();
                    // return String.valueOf(vale).getBytes();
                } else if (colType == ColMeta.COL_TYPE_NEWDECIMAL || colType == ColMeta.COL_TYPE_DECIMAL) {
                    BigDecimal d1 = new BigDecimal(new String(bs));
                    d1 = d1.add(new BigDecimal(new String(bs2)));
                    return String.valueOf(d1).getBytes();
                }
                // continue to count case
                // fallthrough
            case MergeCol.MERGE_COUNT: {
                long s1 = Long.parseLong(new String(bs));
                long s2 = Long.parseLong(new String(bs2));
                long total = s1 + s2;
                return LongUtil.toBytes(total);
            }
            case MergeCol.MERGE_MAX: {
                // System.out.println("value:"+
                // ByteUtil.getNumber(bs).doubleValue());
                // System.out.println("value2:"+
                // ByteUtil.getNumber(bs2).doubleValue());
                // int compare = CompareUtil.compareDouble(ByteUtil.getNumber(bs)
                // .doubleValue(), ByteUtil.getNumber(bs2).doubleValue());
                // return ByteUtil.compareNumberByte(bs, bs2);
                int compare = ByteUtil.compareNumberByte(bs, bs2);
                return (compare > 0) ? bs : bs2;

            }
            case MergeCol.MERGE_MIN: {
                // int compare = CompareUtil.compareDouble(ByteUtil.getNumber(bs)
                // .doubleValue(), ByteUtil.getNumber(bs2).doubleValue());
                // int compare = ByteUtil.compareNumberArray(bs, bs2);
                //return (compare > 0) ? bs2 : bs;
                int compare = ByteUtil.compareNumberByte(bs, bs2);
                return (compare > 0) ? bs2 : bs;
                // return ByteUtil.compareNumberArray2(bs, bs2, 2);
            }
            case MergeCol.MERGE_AVG: {
                if (colType == ColMeta.COL_TYPE_DOUBLE || colType == ColMeta.COL_TYPE_FLOAT) {
                    double aDouble = ByteUtil.getDouble(bs);
                    long s2 = Long.parseLong(new String(bs2));
                    Double vale = aDouble / s2;
                    return vale.toString().getBytes();
                } else if (colType == ColMeta.COL_TYPE_NEWDECIMAL || colType == ColMeta.COL_TYPE_DECIMAL) {
                    BigDecimal sum = new BigDecimal(new String(bs));
                    // mysql avg Precision is the Precision of sum+4,use HALF_UP
                    BigDecimal avg = sum.divide(new BigDecimal(new String(bs2)), sum.scale() + 4, RoundingMode.HALF_UP);
                    return avg.toString().getBytes();
                }
            }
            return null;
            default:
                return null;
        }

    }

    // private static final

    private boolean sameGropuColums(RowDataPacket newRow, RowDataPacket existRow) {
        if (groupColumnIndexs == null) { // select count(*) from aaa , or group
            // column
            return true;
        }
        for (int groupColumnIndex : groupColumnIndexs) {
            if (!Arrays.equals(newRow.fieldValues.get(groupColumnIndex),
                    existRow.fieldValues.get(groupColumnIndex))) {
                return false;
            }

        }
        return true;

    }
}
