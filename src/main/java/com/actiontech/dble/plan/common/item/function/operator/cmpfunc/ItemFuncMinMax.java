/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * min_max's parent
 */
public abstract class ItemFuncMinMax extends ItemFunc {
    ItemResult cmpType;
    String tmpValue;
    int cmpSign;
    boolean compareAsDates;
    Item datetimeItem;

    protected FieldTypes cachedFieldType;

    /*
     * Compare item arguments in the DATETIME context.
     *
     * SYNOPSIS cmpDatetimes() value [out] found least/greatest DATE/DATETIME
     * value
     *
     * DESCRIPTION Compare item arguments as DATETIME values and return the
     * index of the least/greatest argument in the arguments array. The correct
     * integer DATE/DATETIME value of the found argument is stored to the value
     * pointer, if latter is provided.
     *
     * RETURN 0 If one of arguments is NULL or there was a execution error #
     * index of the least/greatest argument
     */
    protected long cmpDatetimes(LongPtr value) {
        long minMax = -1;
        int minMaxIdx = 0;

        for (int i = 0; i < args.size(); i++) {
            long res = args.get(i).valDateTemporal();

            if ((nullValue = args.get(i).isNull()))
                return 0;
            if (i == 0 || (res < minMax ? cmpSign : -cmpSign) > 0) {
                minMax = res;
                minMaxIdx = i;
            }
        }
        value.set(minMax);
        return minMaxIdx;
    }

    protected long cmpTimes(LongPtr value) {
        long minMax = -1;
        int minMaxIdx = 0;

        for (int i = 0; i < args.size(); i++) {
            long res = args.get(i).valTimeTemporal();

            if ((nullValue = args.get(i).isNull()))
                return 0;
            if (i == 0 || (res < minMax ? cmpSign : -cmpSign) > 0) {
                minMax = res;
                minMaxIdx = i;
            }
        }
        value.set(minMax);
        return minMaxIdx;
    }

    public ItemFuncMinMax(List<Item> args, int cmpSignArg) {
        super(args);
        this.cmpSign = cmpSignArg;
        cmpType = ItemResult.INT_RESULT;
        compareAsDates = false;
        datetimeItem = null;
    }

    @Override
    public BigDecimal valReal() {
        double value = 0.0;
        if (compareAsDates) {
            LongPtr result = new LongPtr(0);
            cmpDatetimes(result);
            return new BigDecimal(MyTime.doubleFromDatetimePacked(datetimeItem.fieldType(), result.get()));
        }
        for (int i = 0; i < args.size(); i++) {
            if (i == 0)
                value = args.get(i).valReal().doubleValue();
            else {
                double tmp = args.get(i).valReal().doubleValue();
                if (!args.get(i).isNull() && (tmp < value ? cmpSign : -cmpSign) > 0)
                    value = tmp;
            }
            if ((nullValue = args.get(i).isNull()))
                break;
        }
        return new BigDecimal(value);
    }

    @Override
    public BigInteger valInt() {
        long value = 0;
        if (compareAsDates) {
            LongPtr result = new LongPtr(0);
            cmpDatetimes(result);
            return BigInteger.valueOf(MyTime.longlongFromDatetimePacked(datetimeItem.fieldType(), result.get()));
        }
        /*
         * TS-TODO: val_str decides which type to use using cmp_type. val_int,
     * val_decimal, val_real do not check cmp_type and decide data type
     * according to the method type. This is probably not good:
     *
     * mysql> select least('11', '2'), least('11', '2')+0,
     * concat(least(11,2));
     * +------------------+--------------------+---------------------+ |
     * least('11', '2') | least('11', '2')+0 | concat(least(11,2)) |
     * +------------------+--------------------+---------------------+ | 11
     * | 2 | 2 |
     * +------------------+--------------------+---------------------+ 1 row
     * in set (0.00 sec)
     *
     * Should not the second column return 11? I.e. compare as strings and
     * return '11', then convert to number.
     */
        for (int i = 0; i < args.size(); i++) {
            if (i == 0)
                value = args.get(i).valInt().longValue();
            else {
                long tmp = args.get(i).valInt().longValue();
                if (!args.get(i).isNull() && (tmp < value ? cmpSign : -cmpSign) > 0)
                    value = tmp;
            }
            if ((nullValue = args.get(i).isNull()))
                break;
        }
        return BigInteger.valueOf(value);
    }

    @Override
    public String valStr() {
        if (compareAsDates) {
            if (isTemporal()) {
                /*
                 * In case of temporal data types, we always return string value
                 * according the format of the data type. For example, in case
                 * of LEAST(time_column, datetime_column) the result date type
                 * is DATETIME, so we return a 'YYYY-MM-DD hh:mm:ss' string even
                 * if time_column wins (conversion from TIME to DATETIME happens
                 * in this case).
                 */
                LongPtr result = new LongPtr(0);
                cmpDatetimes(result);
                if (nullValue)
                    return null;
                MySQLTime ltime = new MySQLTime();
                MyTime.timeFromLonglongPacked(ltime, fieldType(), result.get());
                return MyTime.myTimeToStrL(ltime, decimals);

            } else {
                /*
                 * In case of VARCHAR result type we just return val_str() value
                 * of the winning item AS IS, without conversion.
                 */
                long minMaxIdx = cmpDatetimes(new LongPtr(0));
                if (nullValue)
                    return null;
                String strRes = args.get((int) minMaxIdx).valStr();
                if (args.get((int) minMaxIdx).isNullValue()) {
                    // check if the call to val_str() above returns a NULL value
                    nullValue = true;
                    return null;
                }
                return strRes;
            }
        }

        if (cmpType == ItemResult.INT_RESULT) {
            BigInteger nr = valInt();
            if (nullValue)
                return null;
            return nr.toString();
        } else if (cmpType == ItemResult.DECIMAL_RESULT) {
            BigDecimal bd = valDecimal();
            if (nullValue)
                return null;
            return bd.toString();
        } else if (cmpType == ItemResult.REAL_RESULT) {
            BigDecimal nr = valReal();
            if (nullValue)
                return null; /* purecov: inspected */
            return nr.toString();
        } else if (cmpType == ItemResult.STRING_RESULT) {
            String res = null;
            for (int i = 0; i < args.size(); i++) {
                if (i == 0)
                    res = args.get(i).valStr();
                else {
                    String res2 = args.get(i).valStr();
                    if (res2 != null) {
                        int cmp = res.compareTo(res2);
                        if ((cmpSign < 0 ? cmp : -1 * cmp) < 0)
                            res = res2;
                    }
                }
                if ((nullValue = args.get(i).isNull()))
                    return null;
            }
            return res;
        } else { // This case should never be chosen
            return null;
        }
    }

    @Override
    public BigDecimal valDecimal() {
        BigDecimal res = null, tmp;

        if (compareAsDates) {
            LongPtr value = new LongPtr(0);
            cmpDatetimes(value);
            return MyTime.myDecimalFromDatetimePacked(datetimeItem.fieldType(), value.get());
        }
        for (int i = 0; i < args.size(); i++) {
            if (i == 0)
                res = args.get(i).valDecimal();
            else {
                tmp = args.get(i).valDecimal(); // Zero if NULL
                if (tmp != null && tmp.compareTo(res) * cmpSign < 0) {
                    res = tmp;
                }
            }
            if ((nullValue = args.get(i).isNull())) {
                res = null;
                break;
            }
        }
        return res;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        assert (fixed);
        if (compareAsDates) {
            LongPtr result = new LongPtr(0);
            cmpDatetimes(result);
            if (nullValue)
                return true;
            MyTime.timeFromLonglongPacked(ltime, datetimeItem.fieldType(), result.get());
            LongPtr warnings = new LongPtr(0);
            return MyTime.checkDate(ltime, ltime.isNonZeroDate(), fuzzydate, warnings);
        }

        FieldTypes i = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_TIME) {
            return getDateFromTime(ltime);
        } else if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP || i == FieldTypes.MYSQL_TYPE_DATE) {
            assert (false); // Should have been processed in "compare_as_dates"
            // block.

            return getDateFromNonTemporal(ltime, fuzzydate);
        } else {
            return getDateFromNonTemporal(ltime, fuzzydate);
        }
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        assert (fixed);
        if (compareAsDates) {
            LongPtr result = new LongPtr(0);
            cmpDatetimes(result);
            if (nullValue)
                return true;
            MyTime.timeFromLonglongPacked(ltime, datetimeItem.fieldType(), result.get());
            MyTime.datetimeToTime(ltime);
            return false;
        }

        FieldTypes i = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_TIME) {
            LongPtr result = new LongPtr(0);
            cmpTimes(result);
            if (nullValue)
                return true;
            MyTime.timeFromLonglongTimePacked(ltime, result.get());
            return false;
        } else if (i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_TIMESTAMP || i == FieldTypes.MYSQL_TYPE_DATETIME) {
            assert (false); // Should have been processed in "compare_as_dates"
            // block.

            return getTimeFromNonTemporal(ltime);
        } else {
            return getTimeFromNonTemporal(ltime);
        }
    }

    @Override
    public void fixLengthAndDec() {
        int stringArgCount = 0;
        boolean datetimeFound = false;
        decimals = 0;
        maxLength = 0;
        cmpType = args.get(0).temporalWithDateAsNumberResultType();

        for (Item arg : args) {
            maxLength = Math.max(maxLength, arg.getMaxLength());
            decimals = Math.max(decimals, arg.getDecimals());
            cmpType = MySQLcom.itemCmpType(cmpType, arg.temporalWithDateAsNumberResultType());
            if (arg.resultType() == ItemResult.STRING_RESULT)
                stringArgCount++;
            if (arg.resultType() != ItemResult.ROW_RESULT && arg.isTemporalWithDate()) {
                datetimeFound = true;
                if (datetimeItem == null || arg.fieldType() == FieldTypes.MYSQL_TYPE_DATETIME)
                    datetimeItem = arg;
            }
        }

        if (stringArgCount == args.size()) {
            if (datetimeFound) {
                compareAsDates = true;
                /*
                 * We should not do this: cached_field_type=
                 * datetime_item->field_type(); count_datetime_length(args,
                 * arg_count); because compare_as_dates can be TRUE but result
                 * type can still be VARCHAR.
                 */
            }
        }
        cachedFieldType = MySQLcom.aggFieldType(args, 0, args.size());
    }

    @Override
    public ItemResult resultType() {
        return compareAsDates ? ItemResult.STRING_RESULT : cmpType;
    }

    @Override
    public FieldTypes fieldType() {
        return cachedFieldType;
    }

    public ItemResult castToIntType() {
        /*
         * make CAST(LEAST_OR_GREATEST(datetime_expr, varchar_expr)) return a
         * number in format "YYYMMDDhhmmss".
         */
        return compareAsDates ? ItemResult.INT_RESULT : resultType();
    }
}
