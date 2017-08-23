/**
 *
 */
package io.mycat.plan.common.item.function.timefunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalUnit;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.StringPtr;
import io.mycat.plan.common.time.*;

import java.util.ArrayList;
import java.util.List;


public class ItemDateAddInterval extends ItemTemporalHybridFunc {
    StringPtr strValue = new StringPtr("");
    private MySqlIntervalUnit intType;
    private boolean dateSubInterval;

    public ItemDateAddInterval(Item a, Item b, MySqlIntervalUnit type, boolean neg) {
        super(new ArrayList<Item>());
        args.add(a);
        args.add(b);
        this.intType = type;
        this.dateSubInterval = neg;
    }

    @Override
    public final String funcName() {
        return "DATE_ADD";
    }

    @Override
    public void fixLengthAndDec() {
        FieldTypes arg0FieldType;

        maybeNull = true;

        /*
         * The field type for the result of an Item_date function is defined as
         * follows:
         *
         * - If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
         * - If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
         * minutes or seconds then type is MYSQL_TYPE_DATETIME. - Otherwise the
         * result is MYSQL_TYPE_STRING (This is because you can't know if the
         * string contains a DATE, MYSQL_TIME or DATETIME argument)
         */
        arg0FieldType = args.get(0).fieldType();
        int intervalDec = 0;
        if (intType == MySqlIntervalUnit.MICROSECOND || intType == MySqlIntervalUnit.DAY_MICROSECOND
                || intType == MySqlIntervalUnit.HOUR_MICROSECOND || intType == MySqlIntervalUnit.MINUTE_MICROSECOND
                || intType == MySqlIntervalUnit.SECOND_MICROSECOND)
            intervalDec = MyTime.DATETIME_MAX_DECIMALS;
        else if (intType == MySqlIntervalUnit.SECOND && args.get(1).decimals > 0)
            intervalDec = Math.min(args.get(1).decimals, MyTime.DATETIME_MAX_DECIMALS);

        if (arg0FieldType == FieldTypes.MYSQL_TYPE_DATETIME
                || arg0FieldType == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            int dec = Math.max(args.get(0).datetimePrecision(), intervalDec);
            fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, dec);
            cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
        } else if (arg0FieldType == FieldTypes.MYSQL_TYPE_DATE) {
            if (intType == MySqlIntervalUnit.YEAR || intType == MySqlIntervalUnit.QUARTER
                    || intType == MySqlIntervalUnit.MONTH || intType == MySqlIntervalUnit.WEEK
                    || intType == MySqlIntervalUnit.DAY || intType == MySqlIntervalUnit.YEAR_MONTH) {
                cachedFieldType = FieldTypes.MYSQL_TYPE_DATE;
                fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, 0);
            } else {
                cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
                fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, intervalDec);
            }
        } else if (arg0FieldType == FieldTypes.MYSQL_TYPE_TIME) {
            int dec = Math.max(args.get(0).timePrecision(), intervalDec);
            cachedFieldType = FieldTypes.MYSQL_TYPE_TIME;
            fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, dec);
        } else {
            cachedFieldType = FieldTypes.MYSQL_TYPE_STRING;
            /* Behave as a usual string function when return type is VARCHAR. */
            // fix_length_and_charset(MyTime.MAX_DATETIME_FULL_WIDTH);
        }
    }

    /* Here arg[1] is a Item_interval object */
    private boolean get_date_internal(MySQLTime ltime, long fuzzy_date) {
        INTERVAL interval = new INTERVAL();

        if (args.get(0).getDate(ltime, MyTime.TIME_NO_ZERO_DATE)
                || MyTime.get_interval_value(args.get(1), intType, strValue, interval))
            return (nullValue = true);

        if (dateSubInterval)
            interval.neg = !interval.neg;

        /*
         * Make sure we return proper time_type. It's important for val_str().
         */
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_DATE
                && ltime.timeType == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME)
            MyTime.datetime_to_date(ltime);
        else if (cachedFieldType == FieldTypes.MYSQL_TYPE_DATETIME
                && ltime.timeType == MySQLTimestampType.MYSQL_TIMESTAMP_DATE)
            MyTime.date_to_datetime(ltime);

        if ((nullValue = MyTime.date_add_interval(ltime, intType, interval)))
            return true;
        return false;
    }

    private boolean get_time_internal(MySQLTime ltime) {
        INTERVAL interval = new INTERVAL();
        if ((nullValue = args.get(0).getTime(ltime)
                || MyTime.get_interval_value(args.get(1), intType, strValue, interval)))
            return true;

        if (dateSubInterval)
            interval.neg = !interval.neg;

        long usec1 = ((((ltime.day * 24 + ltime.hour) * 60 + ltime.minute) * 60 + ltime.second) * 1000000L
                + ltime.secondPart) * (ltime.neg ? -1 : 1);
        long usec2 = ((((interval.day * 24 + interval.hour) * 60 + interval.minute) * 60 + interval.second) * 1000000L
                + interval.secondPart) * (interval.neg ? -1 : 1);
        long diff = usec1 + usec2;
        LLDivT seconds = new LLDivT();
        seconds.quot = diff / 1000000;
        seconds.rem = diff % 1000000
                * 1000; /* time.second_part= lldiv.rem / 1000 */
        if ((nullValue = (interval.year != 0 || interval.month != 0 || MyTime.sec_to_time(seconds, ltime)))) {
            LOGGER.warn("datetime function overflow!");
            return true;
        }
        return false;
    }

    @Override
    protected boolean val_datetime(MySQLTime ltime, long fuzzy_date) {
        if (cachedFieldType != FieldTypes.MYSQL_TYPE_TIME)
            return get_date_internal(ltime, fuzzy_date | MyTime.TIME_NO_ZERO_DATE);
        return get_time_internal(ltime);
    }

    @Override
    public SQLExpr toExpression() {
        String funcName = funcName();
        if (dateSubInterval) {
            funcName = "date_sub";
        }
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName);
        method.addParameter(args.get(0).toExpression());
        MySqlIntervalExpr intervalExpr = new MySqlIntervalExpr();
        intervalExpr.setValue(args.get(1).toExpression());
        intervalExpr.setUnit(intType);
        method.addParameter(intervalExpr);
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemDateAddInterval(newArgs.get(0), newArgs.get(1), intType, this.dateSubInterval);
    }

}
