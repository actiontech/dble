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
    private MySqlIntervalUnit int_type;

    public ItemFuncTimestampDiff(Item a, Item b, MySqlIntervalUnit type) {
        super(new ArrayList<Item>());
        args.add(a);
        args.add(b);
        this.int_type = type;
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

        if (MyTime.calc_time_diff(ltime2, ltime1, 1, lpseconds, lpmicroseconds))
            neg = -1;

        long seconds = lpseconds.get(), microseconds = lpmicroseconds.get();
        if (int_type == MySqlIntervalUnit.YEAR || int_type == MySqlIntervalUnit.QUARTER
                || int_type == MySqlIntervalUnit.MONTH) {
            long year_beg, year_end, month_beg, month_end, day_beg, day_end;
            long years = 0;
            long second_beg, second_end, microsecond_beg, microsecond_end;

            if (neg == -1) {
                year_beg = ltime2.year;
                year_end = ltime1.year;
                month_beg = ltime2.month;
                month_end = ltime1.month;
                day_beg = ltime2.day;
                day_end = ltime1.day;
                second_beg = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
                second_end = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
                microsecond_beg = ltime2.second_part;
                microsecond_end = ltime1.second_part;
            } else {
                year_beg = ltime1.year;
                year_end = ltime2.year;
                month_beg = ltime1.month;
                month_end = ltime2.month;
                day_beg = ltime1.day;
                day_end = ltime2.day;
                second_beg = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
                second_end = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
                microsecond_beg = ltime1.second_part;
                microsecond_end = ltime2.second_part;
            }

			/* calc years */
            years = year_end - year_beg;
            if (month_end < month_beg || (month_end == month_beg && day_end < day_beg))
                years -= 1;

			/* calc months */
            months = 12 * years;
            if (month_end < month_beg || (month_end == month_beg && day_end < day_beg))
                months += 12 - (month_beg - month_end);
            else
                months += (month_end - month_beg);

            if (day_end < day_beg)
                months -= 1;
            else if ((day_end == day_beg)
                    && ((second_end < second_beg) || (second_end == second_beg && microsecond_end < microsecond_beg)))
                months -= 1;
        }

        if (int_type == MySqlIntervalUnit.YEAR)
            return BigInteger.valueOf(months / 12 * neg);
        if (int_type == MySqlIntervalUnit.QUARTER)
            return BigInteger.valueOf(months / 3 * neg);
        if (int_type == MySqlIntervalUnit.MONTH)
            return BigInteger.valueOf(months * neg);
        if (int_type == MySqlIntervalUnit.WEEK)
            return BigInteger.valueOf(seconds / MyTime.SECONDS_IN_24H / 7L * neg);
        if (int_type == MySqlIntervalUnit.DAY)
            return BigInteger.valueOf(seconds / MyTime.SECONDS_IN_24H * neg);
        if (int_type == MySqlIntervalUnit.HOUR)
            return BigInteger.valueOf(seconds / 3600L * neg);
        if (int_type == MySqlIntervalUnit.MINUTE)
            return BigInteger.valueOf(seconds / 60L * neg);
        if (int_type == MySqlIntervalUnit.SECOND)
            return BigInteger.valueOf(seconds * neg);
        if (int_type == MySqlIntervalUnit.MICROSECOND)
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
        method.addParameter(new SQLIdentifierExpr(int_type.toString()));
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
        return new ItemFuncTimestampDiff(newArgs.get(0), newArgs.get(1), int_type);
    }
}
