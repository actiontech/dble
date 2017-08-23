package io.mycat.plan.common.field.num;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class FieldReal extends FieldNum {
    protected BigDecimal decValue = null;

    public FieldReal(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

    @Override
    public String valStr() {
        internalJob();
        return isNull() ? null : decValue.toString();
    }

    @Override
    public BigInteger valInt() {
        return isNull() ? BigInteger.ZERO : valReal().toBigInteger();
    }

    @Override
    public BigDecimal valReal() {
        internalJob();
        return isNull() ? BigDecimal.ZERO : decValue;
    }

    /**
     * 如果是null的对象则是Item_field_null对象，不会是这些对象，所以这里不会返回null
     */
    @Override
    public BigDecimal valDecimal() {
        return isNull() ? null : valReal();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        internalJob();
        return isNull() ? true : MyTime.myDoubleToDatetimeWithWarn(decValue.doubleValue(), ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        internalJob();
        return isNull() ? true : MyTime.myDoubleToTimeWithWarn(decValue.doubleValue(), ltime);
    }

    @Override
    protected void internalJob() {
        String res = null;
        try {
            res = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.warn("parse string exception!", ue);
        }
        if (res == null)
            decValue = BigDecimal.ZERO;
        else
            try {
                decValue = new BigDecimal(res);
            } catch (Exception e) {
                LOGGER.info("String:" + res + " to BigDecimal exception!", e);
                decValue = BigDecimal.ZERO;
            }
    }

    @Override
    public int compareTo(Field other) {
        if (other == null || !(other instanceof FieldReal)) {
            return 1;
        }
        FieldReal bOther = (FieldReal) other;
        BigDecimal dec = this.valReal();
        BigDecimal dec2 = bOther.valReal();
        if (dec == null && dec2 == null)
            return 0;
        else if (dec2 == null)
            return 1;
        else if (dec == null)
            return -1;
        else
            return dec.compareTo(dec2);
    }

    @Override
    public int compare(byte[] v1, byte[] v2) {
        if (v1 == null && v2 == null)
            return 0;
        else if (v1 == null) {
            return -1;
        } else if (v2 == null) {
            return 1;
        } else
            try {
                String sval1 = MySQLcom.getFullString(charsetName, v1);
                String sval2 = MySQLcom.getFullString(charsetName, v2);
                BigDecimal b1 = new BigDecimal(sval1);
                BigDecimal b2 = new BigDecimal(sval2);
                return b1.compareTo(b2);
            } catch (Exception e) {
                LOGGER.info("String to biginteger exception!", e);
                return -1;
            }
    }
}
