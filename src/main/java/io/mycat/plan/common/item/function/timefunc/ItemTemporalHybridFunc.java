package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.strfunc.ItemStrFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Abstract class for functions returning TIME, DATE, DATETIME or string values,
 * whose data type depends on parameters and is set at fix_field time.
 */
public abstract class ItemTemporalHybridFunc extends ItemStrFunc {
    public ItemTemporalHybridFunc(List<Item> args) {
        super(args);
    }

    protected FieldTypes cachedFieldType; // TIME, DATE, DATETIME or
    // STRING

    /**
     * Get "native" temporal value as MYSQL_TIME
     *
     * @param[out] ltime The value is stored here.
     * @param[in] fuzzy_date Date flags.
     * @retval false On success.
     * @retval true On error.
     */
    protected abstract boolean val_datetime(MySQLTime ltime, long fuzzy_date);

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return cachedFieldType;
    }

    @Override
    public BigInteger valInt() {
        return BigInteger.valueOf(valIntFromDecimal());
    }

    @Override
    public BigDecimal valReal() {
        return valRealFromDecimal();
    }

    @Override
    public BigDecimal valDecimal() {
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_TIME)
            return valDecimalFromTime();
        else if (cachedFieldType == FieldTypes.MYSQL_TYPE_DATETIME)
            return valDecimalFromDate();
        else {
            MySQLTime ltime = new MySQLTime();
            val_datetime(ltime, MyTime.TIME_FUZZY_DATE);
            return nullValue ? BigDecimal.ZERO
                    : ltime.timeType == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ? MyTime.time2my_decimal(ltime)
                    : MyTime.date2my_decimal(ltime);
        }
    }

    @Override
    public String valStr() {
        MySQLTime ltime = new MySQLTime();

        if (val_datetime(ltime, MyTime.TIME_FUZZY_DATE))
            return null;
        String res = MyTime.my_TIME_to_str(ltime, cachedFieldType == FieldTypes.MYSQL_TYPE_STRING
                ? (ltime.secondPart != 0 ? MyTime.DATETIME_MAX_DECIMALS : 0) : decimals);

        if (res == null)
            nullValue = true;
        return res;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        MySQLTime tm = new MySQLTime();
        if (val_datetime(tm, fuzzydate)) {
            assert (nullValue == true);
            return true;
        }
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_TIME
                || tm.timeType == MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
            MyTime.time_to_datetime(tm, ltime);
        else
            ltime = tm;
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        if (val_datetime(ltime, MyTime.TIME_FUZZY_DATE)) {
            assert (nullValue == true);
            return true;
        }
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_TIME
                && ltime.timeType != MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
            MyTime.datetime_to_time(ltime);
        return false;
    }
}
