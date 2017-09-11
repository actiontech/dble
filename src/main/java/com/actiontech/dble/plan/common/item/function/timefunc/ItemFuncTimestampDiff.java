/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalUnit;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class ItemFuncTimestampDiff extends ItemIntFunc {
    private MySqlIntervalUnit intType;

    public ItemFuncTimestampDiff(Item a, Item b, MySqlIntervalUnit type) {
        super(new ArrayList<Item>());
        args.add(a);
        args.add(b);
        this.intType = type;
    }

    @Override
    public final String funcName() {
        return "TIMESTAMPDIFF";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime1 = new MySQLTime();
        MySQLTime ltime2 = new MySQLTime();
        nullValue = false;
        int neg = 1;

        long months = 0;
        if (args.get(0).getDate(ltime1, MyTime.TIME_NO_ZERO_DATE) ||
                args.get(1).getDate(ltime2, MyTime.TIME_NO_ZERO_DATE)) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        LongPtr lpseconds = new LongPtr(0);
        LongPtr lpmicroseconds = new LongPtr(0);

        if (MyTime.calcTimeDiff(ltime2, ltime1, 1, lpseconds, lpmicroseconds))
            neg = -1;

        long seconds = lpseconds.get(), microseconds = lpmicroseconds.get();
        if (intType == MySqlIntervalUnit.YEAR || intType == MySqlIntervalUnit.QUARTER ||
                intType == MySqlIntervalUnit.MONTH) {
            long yearBeg, yearEnd, monthBeg, monthEnd, dayBeg, dayEnd;
            long years = 0;
            long secondBeg, secondEnd, microsecondBeg, microsecondEnd;

            if (neg == -1) {
                yearBeg = ltime2.getYear();
                yearEnd = ltime1.getYear();
                monthBeg = ltime2.getMonth();
                monthEnd = ltime1.getMonth();
                dayBeg = ltime2.getDay();
                dayEnd = ltime1.getDay();
                secondBeg = ltime2.getHour() * 3600 + ltime2.getMinute() * 60 + ltime2.getSecond();
                secondEnd = ltime1.getHour() * 3600 + ltime1.getMinute() * 60 + ltime1.getSecond();
                microsecondBeg = ltime2.getSecondPart();
                microsecondEnd = ltime1.getSecondPart();
            } else {
                yearBeg = ltime1.getYear();
                yearEnd = ltime2.getYear();
                monthBeg = ltime1.getMonth();
                monthEnd = ltime2.getMonth();
                dayBeg = ltime1.getDay();
                dayEnd = ltime2.getDay();
                secondBeg = ltime1.getHour() * 3600 + ltime1.getMinute() * 60 + ltime1.getSecond();
                secondEnd = ltime2.getHour() * 3600 + ltime2.getMinute() * 60 + ltime2.getSecond();
                microsecondBeg = ltime1.getSecondPart();
                microsecondEnd = ltime2.getSecondPart();
            }

            /* calc years */
            years = yearEnd - yearBeg;
            if (monthEnd < monthBeg || (monthEnd == monthBeg && dayEnd < dayBeg))
                years -= 1;

            /* calc months */
            months = 12 * years;
            if (monthEnd < monthBeg || (monthEnd == monthBeg && dayEnd < dayBeg))
                months += 12 - (monthBeg - monthEnd);
            else
                months += (monthEnd - monthBeg);

            if (dayEnd < dayBeg)
                months -= 1;
            else if ((dayEnd == dayBeg) &&
                    ((secondEnd < secondBeg) || (secondEnd == secondBeg && microsecondEnd < microsecondBeg)))
                months -= 1;
        }

        switch (intType) {
            case YEAR:
                return BigInteger.valueOf(months / 12 * neg);
            case QUARTER:
                return BigInteger.valueOf(months / 3 * neg);
            case MONTH:
                return BigInteger.valueOf(months * neg);
            case WEEK:
                return BigInteger.valueOf(seconds / MyTime.SECONDS_IN_24H / 7L * neg);
            case DAY:
                return BigInteger.valueOf(seconds / MyTime.SECONDS_IN_24H * neg);
            case HOUR:
                return BigInteger.valueOf(seconds / 3600L * neg);
            case MINUTE:
                return BigInteger.valueOf(seconds / 60L * neg);
            case SECOND:
                return BigInteger.valueOf(seconds * neg);
            case MICROSECOND:
                /*
                 * In MySQL difference between any two valid datetime values in
                 * microseconds fits into longlong.
                 */
                return BigInteger.valueOf((seconds * 1000000L + microseconds) * neg);
            default:
                nullValue = true;
                return BigInteger.ZERO;
        }
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        method.addParameter(new SQLIdentifierExpr(intType.toString()));
        for (Item arg : args) {
            method.addParameter(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncTimestampDiff(newArgs.get(0), newArgs.get(1), intType);
    }
}
