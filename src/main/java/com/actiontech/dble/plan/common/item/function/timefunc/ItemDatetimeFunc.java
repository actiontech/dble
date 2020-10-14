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


public abstract class ItemDatetimeFunc extends ItemTemporalFunc {

    public ItemDatetimeFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DATETIME;
    }

    @Override
    public BigDecimal valReal() {
        return valRealFromDecimal();
    }

    @Override
    public String valStr() {
        return valStringFromDatetime();
    }

    @Override
    public BigInteger valInt() {
        return BigInteger.valueOf(valIntFromDatetime());
    }

    @Override
    public long valDateTemporal() {
        MySQLTime ltime = new MySQLTime();
        return getDate(ltime, MyTime.TIME_FUZZY_DATE) ? 0L : MyTime.timeToLonglongDatetimePacked(ltime);
    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromDate();
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromDatetime(ltime);
    }

    // All datetime functions must implement get_date()
    // to avoid use of generic Item::get_date()
    // which converts to string and then parses the string as DATETIME.
    public abstract boolean getDate(MySQLTime res, long fuzzyDate);

}
