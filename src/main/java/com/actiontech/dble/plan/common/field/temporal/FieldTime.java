/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.temporal;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

public class FieldTime extends FieldTemporal {

    public FieldTime(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_TIME;
    }

    @Override
    protected void internalJob() {
        String ptrStr = null;
        try {
            ptrStr = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            Field.LOGGER.warn("parse string exception!", ue);
        }
        if (ptrStr != null) {
            MyTime.strToTimeWithWarn(ptrStr, ltime);
        }
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : BigInteger.valueOf(MyTime.timeToUlonglongTime(ltime));
    }

    @Override
    public long valTimeTemporal() {
        internalJob();
        return isNull() ? 0 : MyTime.timeToLonglongTimePacked(ltime);
    }

    @Override
    public long valDateTemporal() {
        internalJob();
        return isNull() ? 0 : MyTime.timeToLonglongDatetimePacked(ltime);
    }

    @Override
    public int compare(byte[] v1, byte[] v2) {
        if (v1 == null && v2 == null)
            return 0;
        try {
            String sval1 = MySQLcom.getFullString(charsetName, v1);
            String sval2 = MySQLcom.getFullString(charsetName, v2);
            MySQLTime ltime1 = new MySQLTime();
            MySQLTime ltime2 = new MySQLTime();
            MyTime.strToTimeWithWarn(sval1, ltime1);
            MyTime.strToTimeWithWarn(sval2, ltime2);
            return ltime1.getCompareResult(ltime2);
        } catch (Exception e) {
            Field.LOGGER.info("String to biginteger exception!", e);
            return -1;
        }
    }

}
