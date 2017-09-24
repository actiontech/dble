/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

public enum FieldTypes {
    MYSQL_TYPE_DECIMAL(0), MYSQL_TYPE_TINY(1), MYSQL_TYPE_SHORT(2), MYSQL_TYPE_LONG(3), MYSQL_TYPE_FLOAT(
            4), MYSQL_TYPE_DOUBLE(5), MYSQL_TYPE_NULL(6), MYSQL_TYPE_TIMESTAMP(7), MYSQL_TYPE_LONGLONG(
            8), MYSQL_TYPE_INT24(9), MYSQL_TYPE_DATE(10), MYSQL_TYPE_TIME(11), MYSQL_TYPE_DATETIME(
            12), MYSQL_TYPE_YEAR(13), MYSQL_TYPE_NEWDATE(14), MYSQL_TYPE_VARCHAR(15), MYSQL_TYPE_BIT(
            16), MYSQL_TYPE_TIMESTAMP2(17), MYSQL_TYPE_DATETIME2(18), MYSQL_TYPE_TIME2(
            19), MYSQL_TYPE_NEWDECIMAL(246), MYSQL_TYPE_ENUM(247), MYSQL_TYPE_SET(
            248), MYSQL_TYPE_TINY_BLOB(249), MYSQL_TYPE_MEDIUM_BLOB(
            250), MYSQL_TYPE_LONG_BLOB(251), MYSQL_TYPE_BLOB(
            252), MYSQL_TYPE_VAR_STRING(253), MYSQL_TYPE_STRING(
            254), MYSQL_TYPE_GEOMETRY(255);

    private int i = 0;

    FieldTypes(int i) {
        this.i = i;
    }

    public int numberValue() {
        return i;
    }

    public static FieldTypes valueOf(int i) {
        if (i < 0 || i >= MYSQL_TYPE_GEOMETRY.i) {
            throw new IndexOutOfBoundsException("Invalid ordinal:" + i);
        } else if (i < MYSQL_TYPE_TIME2.i)
            return values()[i];
        else {
            switch (i) {
                case 246:
                    return MYSQL_TYPE_NEWDECIMAL;
                case 247:
                    return MYSQL_TYPE_ENUM;
                case 248:
                    return MYSQL_TYPE_SET;
                case 249:
                    return MYSQL_TYPE_TINY_BLOB;
                case 250:
                    return MYSQL_TYPE_MEDIUM_BLOB;
                case 251:
                    return MYSQL_TYPE_LONG_BLOB;
                case 252:
                    return MYSQL_TYPE_BLOB;
                case 253:
                    return MYSQL_TYPE_VAR_STRING;
                case 254:
                    return MYSQL_TYPE_STRING;
                case 255:
                    return MYSQL_TYPE_GEOMETRY;
                default:
                    throw new IndexOutOfBoundsException("Invalid ordinal:" + i);
            }
        }
    }

}
