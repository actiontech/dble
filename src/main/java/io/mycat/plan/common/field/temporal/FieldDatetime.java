package io.mycat.plan.common.field.temporal;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

public class FieldDatetime extends FieldTemporalWithDateAndTime {

    public FieldDatetime(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DATETIME;
    }

    @Override
    protected void internalJob() {
        String ptrStr = null;
        try {
            ptrStr = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.warn("parse string exception!", ue);
        }
        if (ptrStr != null) {
            MyTime.strToDatetimeWithWarn(ptrStr, ltime, MyTime.TIME_FUZZY_DATE);
        }
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : BigInteger.valueOf(MyTime.timeToUlonglongDatetime(ltime));
    }

}
