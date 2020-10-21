/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Abstract class for functions returning TIME, DATE, DATETIME or string values,
 * whose data type depends on parameters and is set at fix_field time.
 */
public abstract class ItemTemporalHybridFunc extends ItemStrFunc {
    public ItemTemporalHybridFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
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
    protected abstract boolean valDatetime(MySQLTime ltime, long fuzzyDate);

    @Override
    public Item.ItemResult resultType() {
        return Item.ItemResult.STRING_RESULT;
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
            valDatetime(ltime, MyTime.TIME_FUZZY_DATE);
            return nullValue ? BigDecimal.ZERO :
                    ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ? MyTime.time2MyDecimal(ltime) :
                            MyTime.date2MyDecimal(ltime);
        }
    }

    @Override
    public String valStr() {
        MySQLTime ltime = new MySQLTime();

        if (valDatetime(ltime, MyTime.TIME_FUZZY_DATE))
            return null;
        String res = MyTime.myTimeToStr(ltime, cachedFieldType == FieldTypes.MYSQL_TYPE_STRING ?
                (ltime.getSecondPart() != 0 ? MyTime.DATETIME_MAX_DECIMALS : 0) : decimals);

        if (res == null)
            nullValue = true;
        return res;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        MySQLTime tm = new MySQLTime();
        if (valDatetime(tm, fuzzydate)) {
            assert (nullValue);
            return true;
        }
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_TIME ||
                tm.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
            MyTime.timeToDatetime(tm, ltime);
        else
            MyTime.copy(tm, ltime);
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        if (valDatetime(ltime, MyTime.TIME_FUZZY_DATE)) {
            assert (nullValue);
            return true;
        }
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_TIME &&
                ltime.getTimeType() != MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
            MyTime.datetimeToTime(ltime);
        return false;
    }
}
