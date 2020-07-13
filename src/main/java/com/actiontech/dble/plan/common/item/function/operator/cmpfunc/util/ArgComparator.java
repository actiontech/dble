/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncStrictEqual;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;

import java.math.BigDecimal;
import java.math.BigInteger;


public class ArgComparator {
    private Item a, b;
    private ItemFunc owner;
    private ArgCmpFunc func; // compare function name,function pointer in mysql source code
    double precision = 0.0;
    /* Fields used in DATE/DATETIME comparison. */
    //FieldTypes atype, btype; // Types of a and b items
    boolean isNullsEq; // TRUE <=> compare for the EQUAL_FUNC
    boolean setNull = true; // TRUE <=> set owner->null_value
    // when one of arguments is NULL.
    GetValueFunc getValueAFunc; // get_value_a_func name
    GetValueFunc getValueBFunc; // get_value_b_func name

    private boolean caseInsensitive = false;

    boolean tryYearCmpFunc(Item.ItemResult type) {
        if (type == Item.ItemResult.ROW_RESULT)
            return false;
        boolean aisyear = a.fieldType() == FieldTypes.MYSQL_TYPE_YEAR;
        boolean bisyear = b.fieldType() == FieldTypes.MYSQL_TYPE_YEAR;
        if (!aisyear && !bisyear)
            return false;
        if (aisyear && bisyear) {
            getValueAFunc = new GetYearValue();
            getValueBFunc = new GetYearValue();
        } else if (aisyear && b.isTemporalWithDate()) {
            getValueAFunc = new GetYearValue();
            getValueBFunc = new GetDatetimeValue();
        } else if (bisyear && a.isTemporalWithDate()) {
            getValueBFunc = new GetYearValue();
            getValueAFunc = new GetDatetimeValue();
        } else
            return false;
        isNullsEq = isOwnerEqualFunc();
        func = new CompareDatetime();
        setcmpcontextfordatetime();
        return true;
    }

    /**
     * Check if str_arg is a constant and convert it to datetime packed value.
     * Note, const_value may stay untouched, so the caller is responsible to
     * initialize it.
     *
     * @param dateArg date argument, it's name is used for error reporting.
     * @param strArg  string argument to get datetime value from.
     * @return true on error, false on success, false if str_arg is not a const.
     * @param[out] const_value the converted value is stored here, if not NULL.
     */
    static boolean getDateFromConst(Item dateArg, Item strArg, LongPtr constValue) {
        BoolPtr error = new BoolPtr(false);
        long value = 0;
        if (strArg.fieldType() == FieldTypes.MYSQL_TYPE_TIME) {
            // Convert from TIME to DATETIME
            value = strArg.valDateTemporal();
            if (strArg.isNullValue())
                return true;
        } else {
            // Convert from string to DATETIME
            String strVal = strArg.valStr();
            MySQLTimestampType ttype = (dateArg.fieldType() == FieldTypes.MYSQL_TYPE_DATE ?
                    MySQLTimestampType.MYSQL_TIMESTAMP_DATE :
                    MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            if (strArg.isNullValue()) {
                return true;
            }
            value = MySQLcom.getDateFromStr(strVal, ttype, error);
            if (error.get())
                return true;
        }
        if (constValue != null)
            constValue.set(value);
        return false;
    }

    public ArgComparator() {
    }

    public ArgComparator(Item a, Item b, int charsetIndex) {
        this.a = a;
        this.b = b;
        this.caseInsensitive = CharsetUtil.isCaseInsensitive(charsetIndex);
    }

    public int setCompareFunc(ItemFunc ownerarg, Item.ItemResult type) {
        owner = ownerarg;
        func = comparatorMatrix[type.ordinal()][isOwnerEqualFunc() ? 1 : 0];
        if (type == Item.ItemResult.ROW_RESULT) {
            //TODO
            return 1;
        } else if (type == Item.ItemResult.STRING_RESULT) {
            if (func instanceof CompareString)
                func = new CompareBinaryString(caseInsensitive);
            else if (func instanceof CompareEString)
                func = new CompareEBinaryString(caseInsensitive);
        } else if (type == Item.ItemResult.INT_RESULT) {
            if (a.isTemporal() && b.isTemporal()) {
                func = isOwnerEqualFunc() ? new CompareETimePacked() : new CompareTimePacked();
            } else if (func instanceof CompareIntSigned) {
                //
            } else if (func instanceof CompareEInt) {
                //
            }
        } else if (type == Item.ItemResult.DECIMAL_RESULT) {
            //
        } else if (type == Item.ItemResult.REAL_RESULT) {
            if (a.getDecimals() < Item.NOT_FIXED_DEC && b.getDecimals() < Item.NOT_FIXED_DEC) {
                precision = 5 / Math.pow(10, (Math.max(a.getDecimals(), b.getDecimals()) + 1));
                if (func instanceof CompareReal)
                    func = new CompareRealFixed();
                else if (func instanceof CompareEReal)
                    func = new CompareERealFixed();
            }
        }
        return 0;
    }

    public int setCompareFunc(ItemFunc ownerarg) {
        return setCompareFunc(ownerarg, MySQLcom.itemCmpType(a.resultType(), b.resultType()));
    }

    public int setCmpFunc(ItemFunc ownerarg, Item a1, Item a2, Item.ItemResult type) {
        owner = ownerarg;
        setNull = setNull && (ownerarg != null);
        a = a1;
        b = a2;
        LongPtr constValue = new LongPtr(-1);
        if (canCompareAsDates(a, b, constValue)) {
            //atype = a.fieldType();
            //btype = b.fieldType();
            isNullsEq = isOwnerEqualFunc();
            func = new CompareDatetime();
            getValueAFunc = new GetDatetimeValue();
            getValueBFunc = new GetDatetimeValue();
            setcmpcontextfordatetime();
            return 0;
        } else if (type == Item.ItemResult.STRING_RESULT && a.fieldType() == FieldTypes.MYSQL_TYPE_TIME &&
                b.fieldType() == FieldTypes.MYSQL_TYPE_TIME) {
            isNullsEq = isOwnerEqualFunc();
            func = new CompareDatetime();
            getValueAFunc = new GetTimeValue();
            getValueBFunc = new GetTimeValue();
            setcmpcontextfordatetime();
            return 0;
        } else if (type == Item.ItemResult.STRING_RESULT && a.resultType() == Item.ItemResult.STRING_RESULT &&
                b.resultType() == Item.ItemResult.STRING_RESULT) {
            // see item_cmpfunc.cc line1054
        } else if (tryYearCmpFunc(type)) {
            return 0;
        }
        return setCompareFunc(ownerarg, type);
    }

    public int setCmpFunc(ItemFunc ownerarg, Item a1, Item a2, boolean setnullarg) {
        setNull = setnullarg;
        return setCmpFunc(ownerarg, a1, a2, MySQLcom.itemCmpType(a1.resultType(), a2.resultType()));
    }

    public int compare() {
        return this.func.compare(this);
    }

    public boolean isOwnerEqualFunc() {
        if (this.owner != null)
            return this.owner instanceof ItemFuncStrictEqual;
        return false;
    }

    public void setDatetimeCmpFunc(ItemFunc ownerArg, Item a1, Item a2) {
        owner = ownerArg;
        a = a1;
        b = a2;
        //atype = a.fieldType();
        //btype = b.fieldType();
        isNullsEq = false;
        func = new CompareDatetime();
        getValueAFunc = new GetDatetimeValue();
        getValueBFunc = new GetDatetimeValue();
        setcmpcontextfordatetime();
    }

    /*
     * Check whether compare_datetime() can be used to compare items.
     *
     * SYNOPSIS Arg_comparator::can_compare_as_dates() a, b [in] items to be
     * compared const_value [out] converted value of the string constant, if any
     *
     * DESCRIPTION Check several cases when the DATE/DATETIME comparator should
     * be used. The following cases are checked: 1. Both a and b is a
     * DATE/DATETIME field/function returning string or int result. 2. Only a or
     * b is a DATE/DATETIME field/function returning string or int result and
     * the other item (b or a) is an item with string result. If the second item
     * is a constant one then it's checked to be convertible to the
     * DATE/DATETIME type. If the constant can't be converted to a DATE/DATETIME
     * then the compare_datetime() comparator isn't used and the warning about
     * wrong DATE/DATETIME value is issued. In all other cases
     * (date-[int|real|decimal]/[int|real|decimal]-date) the comparison is
     * handled by other comparators. If the datetime comparator can be used and
     * one the operands of the comparison is a string constant that was
     * successfully converted to a DATE/DATETIME type then the result of the
     * conversion is returned in the const_value if it is provided. If there is
     * no constant or compare_datetime() isn't applicable then the *const_value
     * remains unchanged.
     *
     * @return true if can compare as dates, false otherwise.
     */
    public static boolean canCompareAsDates(Item a, Item b, LongPtr constvalue) {
        if (a.isTemporalWithDate()) {
            if (b.isTemporalWithDate()) /* date[time] + date */ {
                return true;
            } else if (b.resultType() == Item.ItemResult.STRING_RESULT) { // date[time]
                //
                // string
                return !getDateFromConst(a, b, constvalue);
            } else
                return false;
        } else if (b.isTemporalWithDate() && a.resultType() == Item.ItemResult.STRING_RESULT) { // string
            //
            // date[time]
            return !getDateFromConst(b, a, constvalue);
        } else
            return false; // No date[time] items found
    }

    private static ArgCmpFunc[][] comparatorMatrix = {{new CompareString(), new CompareEString()},
            {new CompareReal(), new CompareEReal()}, {new CompareIntSigned(), new CompareEInt()},
            {new CompareRow(), new CompareERow()}, {new CompareDecimal(), new CompareEDecimal()}};

    public void setcmpcontextfordatetime() {
        if (a.isTemporal())
            a.setCmpContext(Item.ItemResult.INT_RESULT);
        if (b.isTemporal())
            b.setCmpContext(Item.ItemResult.INT_RESULT);
    }

    /**
     * compare function
     *
     * @author ActionTech
     */
    private interface ArgCmpFunc {
        int compare(ArgComparator ac);
    }

    private static class CompareString implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            String res1, res2;
            if ((res1 = ac.a.valStr()) != null) {
                if ((res2 = ac.b.valStr()) != null) {
                    if (ac.setNull && ac.owner != null) {
                        ac.owner.setNullValue(false);
                        return res1.compareTo(res2);
                    }
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue(true);
            return ac.a.isNullValue() ? -1 : 1;
        }
    }

    private static class CompareBinaryString implements ArgCmpFunc {
        private boolean caseInsensitive;

        CompareBinaryString(boolean caseInsensitive) {
            this.caseInsensitive = caseInsensitive;
        }
        @Override
        public int compare(ArgComparator ac) {
            String res1, res2;
            if ((res1 = ac.a.valStr()) != null) {
                if ((res2 = ac.b.valStr()) != null) {
                    if (ac.setNull && ac.owner != null)
                        ac.owner.setNullValue((false));
                    if (caseInsensitive) {
                        return String.CASE_INSENSITIVE_ORDER.compare(res1, res2);
                    } else {
                        return res1.compareTo(res2);
                    }
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue((true));
            return ac.a.isNullValue() ? -1 : 1;

        }
    }

    private static class CompareReal implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BigDecimal val1, val2;
            val1 = ac.a.valReal();
            if (!(ac.a.isNull())) {
                val2 = ac.b.valReal();
                if (!(ac.b.isNull())) {
                    if (ac.setNull && ac.owner != null)
                        ac.owner.setNullValue((false));
                    return Integer.compare(val1.compareTo(val2), 0);
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue(true);
            return ac.a.isNullValue() ? -1 : 1;
        }
    }

    private static class CompareDecimal implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BigDecimal val1 = ac.a.valDecimal();
            if (!ac.a.isNull()) {
                BigDecimal val2 = ac.b.valDecimal();
                if (!ac.b.isNull()) {
                    if (ac.setNull && ac.owner != null)
                        ac.owner.setNullValue((false));
                    return val1.compareTo(val2);
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue((true));
            return ac.a.isNullValue() ? -1 : 1;
        }
    }

    private static class CompareIntSigned implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BigInteger val1 = ac.a.valInt();
            if (!ac.a.isNull()) {
                BigInteger val2 = ac.b.valInt();
                if (!ac.b.isNull()) {
                    if (ac.setNull && ac.owner != null)
                        ac.owner.setNullValue((false));
                    return Integer.compare(val1.compareTo(val2), 0);
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue((true));
            return ac.a.isNullValue() ? -1 : 1;
        }
    }

    /**
     * Compare arguments using numeric packed temporal representation.
     */
    private static class CompareTimePacked implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            /*
             * Note, we cannot do this: DBUG_ASSERT((*a)->field_type() ==
             * MYSQL_TYPE_TIME); DBUG_ASSERT((*b)->field_type() ==
             * MYSQL_TYPE_TIME);
             *
             * SELECT col_time_key FROM t1 WHERE col_time_key != UTC_DATE() AND
             * col_time_key = MAKEDATE(43, -2852);
             *
             * is rewritten to:
             *
             * SELECT col_time_key FROM t1 WHERE MAKEDATE(43, -2852) !=
             * UTC_DATE() AND col_time_key = MAKEDATE(43, -2852);
             */
            long val1 = ac.a.valDateTemporal();
            if (!ac.a.isNull()) {
                long val2 = ac.b.valDateTemporal();
                if (!ac.b.isNull()) {
                    if (ac.setNull && ac.owner != null)
                        ac.owner.setNullValue((false));
                    return Long.compare(val1, val2);
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue((true));
            return ac.a.isNullValue() ? -1 : 1;
        }
    }

    private static class CompareETimePacked implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            long val1 = ac.a.valDateTemporal();
            long val2 = ac.b.valDateTemporal();
            if (ac.a.isNull() || ac.b.isNull())
                return (ac.a.isNull() && ac.b.isNull()) ? 1 : 0;
            return (val1 == val2) ? 1 : 0;
        }
    }

    private static class CompareRow implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    private static class CompareEString implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            String res1, res2;
            res1 = ac.a.valStr();
            res2 = ac.b.valStr();
            if (res1 == null || res2 == null)
                return (res1 == null && res2 == null) ? 1 : 0;
            return (res1.compareTo(res2) == 0) ? 1 : 0;
        }
    }

    private static class CompareEBinaryString implements ArgCmpFunc {
        private boolean caseInsensitive;
        CompareEBinaryString(boolean caseInsensitive) {
            this.caseInsensitive = caseInsensitive;
        }
        @Override
        public int compare(ArgComparator ac) {
            String res1, res2;
            res1 = ac.a.valStr();
            res2 = ac.b.valStr();
            if (res1 == null || res2 == null)
                return (res1 == null && res2 == null) ? 1 : 0;
            if (caseInsensitive) {
                return String.CASE_INSENSITIVE_ORDER.compare(res1, res2);
            } else {
                return res1.compareTo(res2);
            }
        }
    }

    private static class CompareEReal implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BigDecimal val1 = ac.a.valReal();
            BigDecimal val2 = ac.b.valReal();
            if (ac.a.isNull() || ac.b.isNull())
                return (ac.a.isNull() && ac.b.isNull()) ? 1 : 0;
            return val1.compareTo(val2) == 0 ? 1 : 0;
        }
    }

    private static class CompareEDecimal implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BigDecimal val1 = ac.a.valDecimal();
            BigDecimal val2 = ac.b.valDecimal();
            if (ac.a.isNull() || ac.b.isNull())
                return (ac.a.isNull() && ac.b.isNull()) ? 1 : 0;
            return (val1.compareTo(val2) == 0) ? 1 : 0;
        }
    }

    private static class CompareEInt implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BigInteger val1 = ac.a.valInt();
            BigInteger val2 = ac.b.valInt();
            if (ac.a.isNull() || ac.b.isNull())
                return (ac.a.isNull() && ac.b.isNull()) ? 1 : 0;
            return val1.compareTo(val2) == 0 ? 1 : 0;
        }
    }

    private static class CompareERow implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    private static class CompareRealFixed implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            /*
             * Fix yet another manifestation of Bug#2338. 'Volatile' will
             * instruct gcc to flush double values out of 80-bit Intel FPU
             * registers before performing the comparison.
             */
            BigDecimal val1, val2;
            val1 = ac.a.valReal();
            if (!ac.a.isNull()) {
                val2 = ac.b.valReal();
                if (!ac.b.isNull()) {
                    if (ac.setNull && ac.owner != null)
                        ac.owner.setNullValue((false));
                    if (val1.compareTo(val2) == 0 || Math.abs(val1.doubleValue() - val2.doubleValue()) < ac.precision)
                        return 0;
                    if (val1.compareTo(val2) < 0)
                        return -1;
                    return 1;
                }
            }
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue((true));
            return ac.a.isNullValue() ? -1 : 1;
        }
    }

    private static class CompareERealFixed implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            double val1 = ac.a.valReal().doubleValue();
            double val2 = ac.b.valReal().doubleValue();
            if (ac.a.isNull() || ac.b.isNull())
                return (ac.a.isNull() && ac.b.isNull()) ? 1 : 0;
            return (val1 == val2 || Math.abs(val1 - val2) < ac.precision) ? 1 : 0;
        }
    }

    /**
     * compare args[0] & args[1] as DATETIMEs SYNOPSIS
     * Arg_comparator::compare_datetime()
     * <p>
     * DESCRIPTION Compare items values as DATE/DATETIME for both EQUAL_FUNC and
     * from other comparison functions. The correct DATETIME values are obtained
     * with help of the get_datetime_value() function.
     * <p>
     * RETURN If is_nulls_eq is TRUE: 1 if items are equal or both are null 0
     * otherwise If is_nulls_eq is FALSE: -1 a < b or at least one item is null
     * 0 a == b 1 a > b See the table: is_nulls_eq | 1 | 1 | 1 | 1 | 0 | 0 | 0 |
     * 0 | a_is_null | 1 | 0 | 1 | 0 | 1 | 0 | 1 | 0 | b_is_null | 1 | 1 | 0 | 0
     * | 1 | 1 | 0 | 0 | result | 1 | 0 | 0 |0/1|-1 |-1 |-1 |-1/0/1|
     *
     * @author ActionTech
     */
    private static class CompareDatetime implements ArgCmpFunc {

        @Override
        public int compare(ArgComparator ac) {
            BoolPtr aIsNull = new BoolPtr(false);
            BoolPtr bIsNull = new BoolPtr(false);
            long aValue, bValue;

            /* Get DATE/DATETIME/TIME value of the 'a' item. */
            aValue = ac.getValueAFunc.get(ac.a, ac.b, aIsNull);
            if (!ac.isNullsEq && aIsNull.get()) {
                if (ac.setNull && ac.owner != null)
                    ac.owner.setNullValue((true));
                return -1;
            }

            /* Get DATE/DATETIME/TIME value of the 'b' item. */
            bValue = ac.getValueBFunc.get(ac.b, ac.a, bIsNull);
            if (aIsNull.get() || bIsNull.get()) {
                if (ac.setNull && ac.owner != null)
                    ac.owner.setNullValue((!ac.isNullsEq));
                return ac.isNullsEq ? (aIsNull.get() == bIsNull.get()) ? 1 : 0 : -1;
            }

            /* Here we have two not-NULL values. */
            if (ac.setNull && ac.owner != null)
                ac.owner.setNullValue((false));

            /* Compare values. */
            if (ac.isNullsEq)
                return aValue == (bValue) ? 1 : 0;
            return Long.compare(aValue, bValue);
        }
    }
}
