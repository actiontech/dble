/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util.CmpUtil;
import com.actiontech.dble.plan.common.ptr.ItemResultPtr;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public class ItemFuncBetweenAnd extends ItemFuncOptNeg {
    private ItemResultPtr cmpType = new ItemResultPtr(null);
    /* TRUE <=> arguments will be compared as dates. */
    boolean compareAsDatesWithStrings;
    boolean compareAsTemporalDates;
    boolean compareAsTemporalTimes;

    /* Comparators used for DATE/DATETIME comparison. */
    ArgComparator geCmp = new ArgComparator();
    ArgComparator leCmp = new ArgComparator();

    public ItemFuncBetweenAnd(Item a, Item b, Item c, boolean isNegation) {
        super(new ArrayList<Item>(), isNegation);
        args.add(a);
        args.add(b);
        args.add(c);
    }

    @Override
    public final String funcName() {
        return "between";
    }

    @Override
    public Functype functype() {
        return Functype.BETWEEN;
    }

    @Override
    public BigInteger valInt() {
        if ((nullValue = args.get(0).isNull())) {
            return BigInteger.ZERO;
        }
        if (compareAsDatesWithStrings) {
            return cmpDatesWithString();
        } else if (cmpType.get() == ItemResult.STRING_RESULT) {
            return cmpString();
        } else if (cmpType.get() == ItemResult.INT_RESULT) {
            return cmpInt();
        } else if (cmpType.get() == ItemResult.DECIMAL_RESULT) {
            return cmpDecimal();
        } else {
            return cmpDouble();
        }

    }

    private BigInteger cmpDouble() {
        double value = args.get(0).valReal().doubleValue();
        double a = args.get(1).valReal().doubleValue();
        double b = args.get(2).valReal().doubleValue();
        if (!args.get(1).isNull() && !args.get(2).isNull()) {
            return (value >= a && value <= b) != negated ? BigInteger.ONE : BigInteger.ZERO;
        }
        return varNullValue(value, a, b);
    }

    private BigInteger cmpDecimal() {
        BigDecimal dec = args.get(0).valDecimal();
        BigDecimal aDec = args.get(1).valDecimal();
        BigDecimal bDec = args.get(2).valDecimal();
        if (!args.get(1).isNull() && !args.get(2).isNull()) {
            return (dec.compareTo(aDec) >= 0 && dec.compareTo(bDec) <= 0) != negated ? BigInteger.ONE : BigInteger.ZERO;
        }
        return varNullValue(dec, aDec, bDec);
    }

    private BigInteger cmpInt() {
        long value = compareAsTemporalTimes ? args.get(0).valTimeTemporal() :
                compareAsTemporalDates ? args.get(0).valDateTemporal() : args.get(0).valInt().longValue();
        long a = getValByItem(args.get(1));
        long b = getValByItem(args.get(2));
        if (!args.get(1).isNull() && !args.get(2).isNull()) {
            return (value >= a && value <= b) != negated ? BigInteger.ONE : BigInteger.ZERO;
        }
        return varNullValue(a, b, value);
    }

    private BigInteger cmpString() {
        String a = args.get(1).valStr();
        String b = args.get(2).valStr();
        String value = args.get(0).valStr();
        if (!args.get(1).isNull() && !args.get(2).isNull()) {
            return (value.compareTo(a) >= 0 && value.compareTo(b) <= 0) != negated ? BigInteger.ONE : BigInteger.ZERO;
        }
        return varNullValue(value, a, b);
    }

    private BigInteger cmpDatesWithString() {
        int geRes = geCmp.compare();
        int leRes = leCmp.compare();
        if (!args.get(1).isNull() && !args.get(2).isNull()) {
            return ((geRes >= 0 && leRes <= 0)) != negated ? BigInteger.ONE : BigInteger.ZERO;
        }
        return varNullValue(geRes, leRes);
    }

    private long getValByItem(Item item) {
        long val;
        if (compareAsTemporalTimes) {
            val = item.valTimeTemporal();
        } else if (compareAsTemporalDates) {
            val = item.valDateTemporal();
        } else {
            val = item.valInt().longValue();
        }
        return val;
    }

    private BigInteger varNullValue(int geRes, int leRes) {
        if (args.get(1).isNull()) {
            nullValue = leRes > 0; // not null if false range.
        } else {
            nullValue = geRes < 0;
        }
        return !nullValue ? BigInteger.ONE : BigInteger.ZERO;
    }

    private BigInteger varNullValue(double value, double a, double b) {
        if (args.get(1).isNull() && args.get(2).isNull())
            nullValue = true;
        else if (args.get(1).isNull()) {
            nullValue = value <= b; // not null if false range.
        } else {
            nullValue = value >= a;
        }
        return !nullValue ? BigInteger.ONE : BigInteger.ZERO;
    }

    private BigInteger varNullValue(BigDecimal dec, BigDecimal aDec, BigDecimal bDec) {
        if (args.get(1).isNull() && args.get(2).isNull()) {
            nullValue = true;
        } else if (args.get(1).isNull()) {
            nullValue = dec.compareTo(bDec) <= 0;
        } else {
            nullValue = dec.compareTo(aDec) >= 0;
        }
        return !nullValue ? BigInteger.ONE : BigInteger.ZERO;
    }

    private BigInteger varNullValue(long a, long b, long value) {
        if (args.get(1).isNull() && args.get(2).isNull()) {
            nullValue = true;
        } else if (args.get(1).isNull()) {
            nullValue = value <= b; // not null if false range.
        } else {
            nullValue = value >= a;
        }
        return !nullValue ? BigInteger.ONE : BigInteger.ZERO;
    }

    private BigInteger varNullValue(String value, String a, String b) {
        if (args.get(1).isNull() && args.get(2).isNull()) {
            nullValue = true;
        } else if (args.get(1).isNull()) {
            // Set to not null if false range.
            nullValue = value.compareTo(b) <= 0;
        } else {
            // Set to not null if false range.
            nullValue = value.compareTo(a) >= 0;
        }
        return !nullValue ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public boolean fixFields() {
        return super.fixFields();

    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 1;
        int i;
        int datetimeItemsFound = 0;
        int timeItemsFound = 0;
        compareAsDatesWithStrings = false;
        compareAsTemporalTimes = compareAsTemporalDates = false;
        /*
         * As some compare functions are generated after sql_yacc, we have to
         * check for out of memory conditions here
         */
        if (args.get(0) == null || args.get(1) == null || args.get(2) == null)
            return;
        if (CmpUtil.aggCmpType(cmpType, args, 3) != 0)
            return;
        /*
         * Detect the comparison of DATE/DATETIME items. At least one of items
         * should be a DATE/DATETIME item and other items should return the
         * STRING result.
         */
        if (cmpType.get() == ItemResult.STRING_RESULT) {
            for (i = 0; i < 3; i++) {
                if (args.get(i).isTemporalWithDate())
                    datetimeItemsFound++;
                else if (args.get(i).fieldType() == FieldTypes.MYSQL_TYPE_TIME)
                    timeItemsFound++;
            }
        }

        if (datetimeItemsFound + timeItemsFound == 3) {
            if (timeItemsFound == 3) {
                // All items are TIME
                cmpType.set(ItemResult.INT_RESULT);
                compareAsTemporalTimes = true;
            } else {
                /*
                 * There is at least one DATE or DATETIME item, all other items
                 * are DATE, DATETIME or TIME.
                 */
                cmpType.set(ItemResult.INT_RESULT);
                compareAsTemporalDates = true;
            }
        } else if (datetimeItemsFound > 0) {
            /*
             * There is at least one DATE or DATETIME item. All other items are
             * DATE, DATETIME or strings.
             */
            compareAsDatesWithStrings = true;
            geCmp.setDatetimeCmpFunc(this, args.get(0), args.get(1));
            leCmp.setDatetimeCmpFunc(this, args.get(0), args.get(2));
        } else if (args.get(0).type().equals(ItemType.FIELD_ITEM)) {
            //in fact ,it will not reach here
            throw new RuntimeException("not supportted yet!");
        }
    }

    @Override
    public int decimalPrecision() {
        return 1;
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr first = args.get(0).toExpression();
        SQLExpr second = args.get(1).toExpression();
        SQLExpr third = args.get(2).toExpression();
        return new SQLBetweenExpr(first, this.negated, second, third);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncBetweenAnd(newArgs.get(0), newArgs.get(1), newArgs.get(2), this.negated);
    }
}
