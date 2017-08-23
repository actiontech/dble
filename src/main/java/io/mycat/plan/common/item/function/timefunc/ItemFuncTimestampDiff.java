package io.mycat.plan.common.item.function.timefunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalUnit;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

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
        if (args.get(0).getDate(ltime1, MyTime.TIME_NO_ZERO_DATE)
                || args.get(1).getDate(ltime2, MyTime.TIME_NO_ZERO_DATE)) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        LongPtr lpseconds = new LongPtr(0);
        LongPtr lpmicroseconds = new LongPtr(0);

        if (MyTime.calcTimeDiff(ltime2, ltime1, 1, lpseconds, lpmicroseconds))
            neg = -1;

        long seconds = lpseconds.get(), microseconds = lpmicroseconds.get();
        if (intType == MySqlIntervalUnit.YEAR || intType == MySqlIntervalUnit.QUARTER
                || intType == MySqlIntervalUnit.MONTH) {
            long yearBeg, yearEnd, monthBeg, monthEnd, dayBeg, dayEnd;
            long years = 0;
            long secondBeg, secondEnd, microsecondBeg, microsecondEnd;

            if (neg == -1) {
                yearBeg = ltime2.year;
                yearEnd = ltime1.year;
                monthBeg = ltime2.month;
                monthEnd = ltime1.month;
                dayBeg = ltime2.day;
                dayEnd = ltime1.day;
                secondBeg = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
                secondEnd = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
                microsecondBeg = ltime2.secondPart;
                microsecondEnd = ltime1.secondPart;
            } else {
                yearBeg = ltime1.year;
                yearEnd = ltime2.year;
                monthBeg = ltime1.month;
                monthEnd = ltime2.month;
                dayBeg = ltime1.day;
                dayEnd = ltime2.day;
                secondBeg = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
                secondEnd = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
                microsecondBeg = ltime1.secondPart;
                microsecondEnd = ltime2.secondPart;
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
            else if ((dayEnd == dayBeg)
                    && ((secondEnd < secondBeg) || (secondEnd == secondBeg && microsecondEnd < microsecondBeg)))
                months -= 1;
        }

        if (intType == MySqlIntervalUnit.YEAR)
            return BigInteger.valueOf(months / 12 * neg);
        if (intType == MySqlIntervalUnit.QUARTER)
            return BigInteger.valueOf(months / 3 * neg);
        if (intType == MySqlIntervalUnit.MONTH)
            return BigInteger.valueOf(months * neg);
        if (intType == MySqlIntervalUnit.WEEK)
            return BigInteger.valueOf(seconds / MyTime.SECONDS_IN_24H / 7L * neg);
        if (intType == MySqlIntervalUnit.DAY)
            return BigInteger.valueOf(seconds / MyTime.SECONDS_IN_24H * neg);
        if (intType == MySqlIntervalUnit.HOUR)
            return BigInteger.valueOf(seconds / 3600L * neg);
        if (intType == MySqlIntervalUnit.MINUTE)
            return BigInteger.valueOf(seconds / 60L * neg);
        if (intType == MySqlIntervalUnit.SECOND)
            return BigInteger.valueOf(seconds * neg);
        if (intType == MySqlIntervalUnit.MICROSECOND)
            /*
             * In MySQL difference between any two valid datetime values in
             * microseconds fits into longlong.
             */
            return BigInteger.valueOf((seconds * 1000000L + microseconds) * neg);
        else {
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
