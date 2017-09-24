/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

public class MergeCol {
    public static final int MERGE_COUNT = 1;
    public static final int MERGE_SUM = 2;
    public static final int MERGE_MIN = 3;
    public static final int MERGE_MAX = 4;
    public static final int MERGE_AVG = 5;
    public static final int MERGE_UNSUPPORT = -1;
    public static final int MERGE_NOMERGE = -2;
    public final int mergeType;
    public final ColMeta colMeta;

    public MergeCol(ColMeta colMeta, int mergeType) {
        super();
        this.colMeta = colMeta;
        this.mergeType = mergeType;
    }

    public static int getMergeType(String mergeType) {
        String upper = mergeType.toUpperCase();
        if (upper.startsWith("COUNT")) {
            return MERGE_COUNT;
        } else if (upper.startsWith("SUM")) {
            return MERGE_SUM;
        } else if (upper.startsWith("MIN")) {
            return MERGE_MIN;
        } else if (upper.startsWith("MAX")) {
            return MERGE_MAX;
        } else if (upper.startsWith("AVG")) {
            return MERGE_AVG;
        } else {
            return MERGE_UNSUPPORT;
        }
    }

    public static int tryParseAggCol(String column) {
        // MIN(*),MAX(*),COUNT(*),SUM
        if (column.length() < 6) {
            return -1;
        }
        column = column.toUpperCase();

        if (column.startsWith("COUNT(")) {
            return MERGE_COUNT;
        } else if (column.startsWith("SUM(")) {
            return MERGE_SUM;
        } else if (column.startsWith("MIN(")) {
            return MERGE_MIN;
        } else if (column.startsWith("MAX(")) {
            return MERGE_MAX;
        } else if (column.startsWith("AVG(")) {
            return MERGE_AVG;
        } else {
            return MERGE_NOMERGE;
        }
    }
}
