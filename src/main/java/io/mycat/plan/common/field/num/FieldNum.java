package io.mycat.plan.common.field.num;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.field.FieldUtil;
import io.mycat.plan.common.item.Item.ItemResult;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 整数的基类
 *
 * @author ActionTech
 */
public abstract class FieldNum extends Field {

    protected BigInteger intValue = null;
    protected String zeroptrStr = null;

    public FieldNum(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
        zerofill = (FieldUtil.ZEROFILL_FLAG & flags) != 0;
//        unsigned_flag = (FieldUtil.UNSIGNED_FLAG & flags) != 0;
    }

    public boolean zerofill = false;
//    public boolean unsigned_flag = false;

    @Override
    public ItemResult resultType() {
        return ItemResult.REAL_RESULT;
    }

    @Override
    public String valStr() {
        internalJob();
        return isNull() ? null : zeroptrStr;
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : intValue;
    }

    @Override
    public BigDecimal valReal() {
        internalJob();
        return isNull() ? BigDecimal.ZERO : new BigDecimal(intValue);
    }

    @Override
    public BigDecimal valDecimal() {
        internalJob();
        return isNull() ? null : new BigDecimal(intValue);
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        internalJob();
        return isNull() ? true : MyTime.myLonglongToDatetimeWithWarn(intValue.longValue(), ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        internalJob();
        return isNull() ? true : MyTime.myLonglongToTimeWithWarn(intValue.longValue(), ltime);
    }

    @Override
    protected void internalJob() {
        /** --计算出zero_ptrstr-- **/
        String res = null;
        try {
            res = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.warn("parse string exception!", ue);
        }
        if (res != null)
            if (zerofill && res.length() < fieldLength) {
                for (int i = 0; i < fieldLength - res.length(); i++) {
                    res = "0" + res;
                }
            }
        zeroptrStr = res;

        /** -- 计算出intValue -- **/

        if (res == null)
            intValue = BigInteger.ZERO;
        else
            try {
                intValue = new BigInteger(res);
            } catch (Exception e) {
                LOGGER.info("String:" + res + " to BigInteger exception!", e);
                intValue = BigInteger.ZERO;
            }
    }

    @Override
    public int compareTo(Field other) {
        if (other == null || !(other instanceof FieldNum)) {
            return 1;
        }
        FieldNum bOther = (FieldNum) other;
        BigInteger intValue = this.valInt();
        BigInteger intValue2 = bOther.valInt();
        if (intValue == null && intValue2 == null)
            return 0;
        else if (intValue2 == null)
            return 1;
        else if (intValue == null)
            return -1;
        else
            return intValue.compareTo(intValue2);
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
            return FieldUtil.compareIntUsingStringBytes(v1, v2);
    }

}
