/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.time;

public enum MySQLTimestampType {

    MYSQL_TIMESTAMP_NONE(-2), MYSQL_TIMESTAMP_ERROR(-1), MYSQL_TIMESTAMP_DATE(0), MYSQL_TIMESTAMP_DATETIME(
            1), MYSQL_TIMESTAMP_TIME(2);
    private int i;

    MySQLTimestampType(int i) {
        this.i = i;
    }

    public static MySQLTimestampType valueOf(int i) {
        if (i < 0 || i >= values().length) {
            throw new IndexOutOfBoundsException("Invalid ordinal");
        }
        return values()[i];
    }

    public String toString() {
        return String.valueOf(i);
    }
}
