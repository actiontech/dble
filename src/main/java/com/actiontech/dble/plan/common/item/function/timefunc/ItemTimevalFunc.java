/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.Timeval;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/*
 Abstract class for functions returning "struct timeval".
 */
public abstract class ItemTimevalFunc extends ItemFunc {

    public ItemTimevalFunc(List<Item> args) {
        super(args);
    }

    /**
     * Return timestamp in "struct timeval" format.
     *
     * @param[out] tm The value is store here.
     * @retval false On success
     * @retval true On error
     */
    public abstract boolean valTimeval(Timeval tm);

    @Override
    public BigDecimal valReal() {
        Timeval tm = new Timeval();
        return valTimeval(tm) ? BigDecimal.ZERO :
                BigDecimal.valueOf((double) tm.getTvSec() + (double) tm.getTvUsec() / (double) 1000000);
    }

    @Override
    public BigInteger valInt() {
        Timeval tm = new Timeval();
        return valTimeval(tm) ? BigInteger.ZERO : BigInteger.valueOf(tm.getTvSec());
    }

    @Override
    public BigDecimal valDecimal() {
        Timeval tm = new Timeval();
        return valTimeval(tm) ? null : tm.timeval2MyDecimal();
    }

    @Override
    public String valStr() {
        Timeval tm = new Timeval();
        if (valTimeval(tm))
            return null;
        return tm.myTimevalToStr();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return getDateFromNumeric(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromNumeric(ltime);
    }

    public ItemResult resultType() {
        return decimals != 0 ? ItemResult.DECIMAL_RESULT : ItemResult.INT_RESULT;
    }

}
