/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.temporal;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

public class FieldDate extends FieldTemporaWithDate {

    public FieldDate(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DATE;
    }

    @Override
    protected void internalJob() {
        String ptrStr = null;
        try {
            ptrStr = MySQLcom.getFullString(javaCharsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            Field.LOGGER.info("parse string exception!", ue);
        }
        if (ptrStr != null) {
            MyTime.strToDatetimeWithWarn(ptrStr, ltime, MyTime.TIME_FUZZY_DATE);
        }
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : BigInteger.valueOf(MyTime.timeToUlonglongDate(ltime));
    }


}
