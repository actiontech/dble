package io.mycat.plan.common.field.temporal;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

public class FieldTime extends FieldTemporal {

    public FieldTime(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_TIME;
    }

    @Override
    protected void internalJob() {
        String ptr_str = null;
        try {
            ptr_str = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            logger.warn("parse string exception!", ue);
        }
        if (ptr_str != null) {
            MyTime.str_to_time_with_warn(ptr_str, ltime);
        }
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : BigInteger.valueOf(MyTime.TIME_to_ulonglong_time(ltime));
    }

    @Override
    public long valTimeTemporal() {
        internalJob();
        return isNull() ? 0 : MyTime.TIME_to_longlong_time_packed(ltime);
    }

    @Override
    public long valDateTemporal() {
        internalJob();
        return isNull() ? 0 : MyTime.TIME_to_longlong_datetime_packed(ltime);
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
            MyTime.str_to_time_with_warn(sval1, ltime1);
            MyTime.str_to_time_with_warn(sval2, ltime2);
            return ltime1.getCompareResult(ltime2);
        } catch (Exception e) {
            logger.info("String to biginteger exception!", e);
            return -1;
        }
    }

}
