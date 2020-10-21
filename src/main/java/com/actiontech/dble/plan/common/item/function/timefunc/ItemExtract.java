/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;

import java.math.BigInteger;
import java.util.List;

public class ItemExtract extends ItemIntFunc {
    private SQLIntervalUnit intType;
    private boolean dateValue;

    public ItemExtract(Item a, SQLIntervalUnit intType, int charsetIndex) {
        super(a, charsetIndex);
        this.intType = intType;
    }

    @Override
    public final String funcName() {
        return "extract";
    }

    @Override
    public Functype functype() {
        return Functype.EXTRACT_FUNC;
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true; // If wrong date
        switch (intType) {
            case YEAR:
                maxLength = 4;
                dateValue = true;
                break;
            case YEAR_MONTH:
                maxLength = 6;
                dateValue = true;
                break;
            case QUARTER:
                maxLength = 2;
                dateValue = true;
                break;
            case MONTH:
                maxLength = 2;
                dateValue = true;
                break;
            case WEEK:
                maxLength = 2;
                dateValue = true;
                break;
            case DAY:
                maxLength = 2;
                dateValue = true;
                break;
            case DAY_HOUR:
                maxLength = 9;
                dateValue = false;
                break;
            case DAY_MINUTE:
                maxLength = 11;
                dateValue = false;
                break;
            case DAY_SECOND:
                maxLength = 13;
                dateValue = false;
                break;
            case HOUR:
                maxLength = 2;
                dateValue = false;
                break;
            case HOUR_MINUTE:
                maxLength = 4;
                dateValue = false;
                break;
            case HOUR_SECOND:
                maxLength = 6;
                dateValue = false;
                break;
            case MINUTE:
                maxLength = 2;
                dateValue = false;
                break;
            case MINUTE_SECOND:
                maxLength = 4;
                dateValue = false;
                break;
            case SECOND:
                maxLength = 2;
                dateValue = false;
                break;
            case MICROSECOND:
                maxLength = 2;
                dateValue = false;
                break;
            case DAY_MICROSECOND:
                maxLength = 20;
                dateValue = false;
                break;
            case HOUR_MICROSECOND:
                maxLength = 13;
                dateValue = false;
                break;
            case MINUTE_MICROSECOND:
                maxLength = 11;
                dateValue = false;
                break;
            case SECOND_MICROSECOND:
                maxLength = 9;
                dateValue = false;
                break;
            default:
                break;
        }
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime = new MySQLTime();
        int weekFormat;
        long neg;
        if (dateValue) {
            if (getArg0Date(ltime, MyTime.TIME_FUZZY_DATE))
                return BigInteger.ZERO;
            neg = 1;
        } else {
            if (getArg0Time(ltime))
                return BigInteger.ZERO;
            neg = ltime.isNeg() ? -1 : 1;
        }
        switch (intType) {
            case YEAR:
                return BigInteger.valueOf(ltime.getYear());
            case YEAR_MONTH:
                return BigInteger.valueOf(ltime.getYear() * 100L + ltime.getMonth());
            case QUARTER:
                return BigInteger.valueOf((ltime.getMonth() + 2) / 3);
            case MONTH:
                return BigInteger.valueOf(ltime.getMonth());
            case WEEK: {
                weekFormat = MyTime.WEEK_MONDAY_FIRST;
                long ret = MyTime.calcWeek(ltime, MyTime.weekMode(weekFormat), new LongPtr(0));
                return BigInteger.valueOf(ret);

            }
            case DAY:
                return BigInteger.valueOf(ltime.getDay());
            case DAY_HOUR:
                return BigInteger.valueOf((ltime.getDay() * 100L + ltime.getHour()) * neg);
            case DAY_MINUTE:
                return (BigInteger.valueOf((ltime.getDay() * 10000L + ltime.getHour() * 100L + ltime.getMinute()) * neg));
            case DAY_SECOND:
                return BigInteger.valueOf(
                        (ltime.getDay() * 1000000L + (ltime.getHour() * 10000L + ltime.getMinute() * 100 + ltime.getSecond())) * neg);
            case HOUR:
                return BigInteger.valueOf(ltime.getHour() * neg);
            case HOUR_MINUTE:
                return BigInteger.valueOf((ltime.getHour() * 100 + ltime.getMinute()) * neg);
            case HOUR_SECOND:
                return BigInteger.valueOf((ltime.getHour() * 10000 + ltime.getMinute() * 100 + ltime.getSecond()) * neg);
            case MINUTE:
                return BigInteger.valueOf(ltime.getMinute() * neg);
            case MINUTE_SECOND:
                return BigInteger.valueOf((ltime.getMinute() * 100 + ltime.getSecond()) * neg);
            case SECOND:
                return BigInteger.valueOf(ltime.getSecond() * neg);
            case MICROSECOND:
                return BigInteger.valueOf(ltime.getSecondPart() * neg);
            case DAY_MICROSECOND:
                return BigInteger.valueOf(
                        ((ltime.getDay() * 1000000L + ltime.getHour() * 10000L + ltime.getMinute() * 100 + ltime.getSecond()) * 1000000L +
                                ltime.getSecondPart()) * neg);
            case HOUR_MICROSECOND:
                return BigInteger.valueOf(
                        ((ltime.getHour() * 10000L + ltime.getMinute() * 100 + ltime.getSecond()) * 1000000L + ltime.getSecondPart()) * neg);
            case MINUTE_MICROSECOND:
                return BigInteger.valueOf((((ltime.getMinute() * 100 + ltime.getSecond())) * 1000000L + ltime.getSecondPart()) * neg);
            case SECOND_MICROSECOND:
                return BigInteger.valueOf((ltime.getSecond() * 1000000L + ltime.getSecondPart()) * neg);
            default:
                break;
        }
        return BigInteger.ZERO; // Impossible
    }

    @Override
    public SQLExpr toExpression() {
        MySqlExtractExpr extract = new MySqlExtractExpr();
        extract.setValue(args.get(0).toExpression());
        extract.setUnit(intType);
        return extract;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemExtract(newArgs.get(0), intType, charsetIndex);
    }
}
