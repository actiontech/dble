/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


/**
 * Abstract class for functions returning DATE values.
 */
public abstract class ItemDateFunc extends ItemTemporalFunc {

    public ItemDateFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DATE;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromDate(ltime);
    }

    @Override
    public String valStr() {
        return valStringFromDate();
    }

    @Override
    public BigInteger valInt() {
        return BigInteger.valueOf(valIntFromDate());
    }

    @Override
    public long valDateTemporal() {
        MySQLTime ltime = new MySQLTime();
        return getDate(ltime, MyTime.TIME_FUZZY_DATE) ? 0 : (MyTime.timeToLonglongDatePacked(ltime));
    }

    @Override
    public BigDecimal valReal() {
        return new BigDecimal(valInt());
    }

    @Override
    public void fixLengthAndDec() {
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, 0);
    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromDate();
    }

    // All date functions must implement get_date()
    // to avoid use of generic Item::get_date()
    // which converts to string and then parses the string as DATE.
    public abstract boolean getDate(MySQLTime res, long fuzzyDate);
}
