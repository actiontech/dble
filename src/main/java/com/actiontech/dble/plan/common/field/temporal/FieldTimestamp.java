/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.temporal;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

public class FieldTimestamp extends FieldTemporalWithDateAndTime {

    public FieldTimestamp(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_TIMESTAMP;
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : BigInteger.valueOf(MyTime.timeToUlonglongDatetime(ltime));
    }

    @Override
    protected void internalJob() {
        String ptrStr = null;
        try {
            ptrStr = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.warn("parse string exception!", ue);
        }
        if (ptrStr != null)
            MyTime.strToDatetimeWithWarn(ptrStr, ltime, MyTime.TIME_FUZZY_DATE);
    }


}
