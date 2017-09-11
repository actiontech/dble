/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

import java.io.Serializable;

public class ColMeta implements Serializable {
    public static final int COL_TYPE_DECIMAL = 0;
    public static final int COL_TYPE_INT = 1;
    public static final int COL_TYPE_SHORT = 2;
    public static final int COL_TYPE_LONG = 3;
    public static final int COL_TYPE_FLOAT = 4;
    public static final int COL_TYPE_DOUBLE = 5;
    public static final int COL_TYPE_NULL = 6;
    public static final int COL_TYPE_TIMSTAMP = 7;
    public static final int COL_TYPE_LONGLONG = 8;
    public static final int COL_TYPE_INT24 = 9;
    public static final int COL_TYPE_DATE = 0x0a;
    public static final int COL_TYPE_DATETIME = 0X0C;
    public static final int COL_TYPE_TIME = 0x0b;
    public static final int COL_TYPE_YEAR = 0x0d;
    public static final int COL_TYPE_NEWDATE = 0x0e;
    public static final int COL_TYPE_VACHAR = 0x0f;
    public static final int COL_TYPE_BIT = 0x10;
    public static final int COL_TYPE_NEWDECIMAL = 0xf6;
    public static final int COL_TYPE_ENUM = 0xf7;
    public static final int COL_TYPE_SET = 0xf8;
    public static final int COL_TYPE_TINY_BLOB = 0xf9;
    public static final int COL_TYPE_TINY_TYPE_MEDIUM_BLOB = 0xfa;
    public static final int COL_TYPE_TINY_TYPE_LONG_BLOB = 0xfb;
    public static final int COL_TYPE_BLOB = 0xfc;
    public static final int COL_TYPE_VAR_STRING = 0xfd;
    public static final int COL_TYPE_STRING = 0xfe;
    public static final int COL_TYPE_GEOMETRY = 0xff;
    private int colIndex;
    private final int colType;

    private int decimals;

    private int avgSumIndex;
    private int avgCountIndex;

    public ColMeta(int colIndex, int colType) {
        super();
        this.colIndex = colIndex;
        this.colType = colType;
    }

    public ColMeta(int avgSumIndex, int avgCountIndex, int colType) {
        super();
        this.avgSumIndex = avgSumIndex;
        this.avgCountIndex = avgCountIndex;
        this.colType = colType;
    }

    public int getColIndex() {
        return colIndex;
    }

    public int getColType() {
        return colType;
    }

    @Override
    public String toString() {
        return "ColMeta [colIndex=" + colIndex + ", colType=" + colType + "]";
    }

    public void setColIndex(int colIndex) {
        this.colIndex = colIndex;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }

    public int getAvgSumIndex() {
        return avgSumIndex;
    }

    public void setAvgSumIndex(int avgSumIndex) {
        this.avgSumIndex = avgSumIndex;
    }

    public int getAvgCountIndex() {
        return avgCountIndex;
    }

    public void setAvgCountIndex(int avgCountIndex) {
        this.avgCountIndex = avgCountIndex;
    }
}
