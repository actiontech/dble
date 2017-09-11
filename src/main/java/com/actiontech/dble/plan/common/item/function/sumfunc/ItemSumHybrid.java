/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public abstract class ItemSumHybrid extends ItemSum {

    protected Field value;
    protected ItemResult hybridType;
    protected FieldTypes hybridFieldType;
    protected int cmpSign;
    protected boolean wasValues; // Set if we have found at least one row (for
    // max/min only)

    public ItemSumHybrid(List<Item> args, int sign, boolean isPushDown, List<Field> fields) {
        super(args, isPushDown, fields);
        hybridFieldType = FieldTypes.MYSQL_TYPE_LONGLONG;
        hybridType = ItemResult.INT_RESULT;
        cmpSign = sign;
        wasValues = true;
    }

    @Override
    public boolean fixFields() {
        Item item = args.get(0);

        // 'item' can be changed during fix_fields
        if (!item.isFixed() && item.fixFields())
            return true;
        item = args.get(0);
        decimals = item.getDecimals();
        value = Field.getFieldItem(funcName(), null, item.fieldType().numberValue(), item.getCharsetIndex(),
                item.getMaxLength(), item.getDecimals(), (item.isMaybeNull() ? 0 : FieldUtil.NOT_NULL_FLAG));

        ItemResult i = hybridType = item.resultType();
        if (i == ItemResult.INT_RESULT || i == ItemResult.DECIMAL_RESULT || i == ItemResult.STRING_RESULT) {
            maxLength = item.getMaxLength();

        } else if (i == ItemResult.REAL_RESULT) {
            maxLength = floatLength(decimals);

        } else {
            assert (false);
        }
        charsetIndex = item.getCharsetIndex();
        /*
         * MIN/MAX can return NULL for empty set indepedent of the used column
         */
        maybeNull = true;
        nullValue = true;
        fixLengthAndDec();
        hybridFieldType = item.fieldType();

        fixed = true;
        return false;
    }

    @Override
    public void clear() {
        value.setPtr(null);
        nullValue = true;
    }

    @Override
    public BigDecimal valReal() {
        if (nullValue)
            return BigDecimal.ZERO;
        BigDecimal retval = value.valReal();
        if (nullValue = value.isNull())
            retval = BigDecimal.ZERO;
        return retval;
    }

    @Override
    public BigInteger valInt() {
        if (nullValue)
            return BigInteger.ZERO;
        BigInteger retval = value.valInt();
        if (nullValue = value.isNull())
            retval = BigInteger.ZERO;
        return retval;
    }

    @Override
    public long valTimeTemporal() {
        if (nullValue)
            return 0;
        long retval = value.valTimeTemporal();
        if (nullValue = value.isNull())
            retval = 0;
        return retval;
    }

    @Override
    public long valDateTemporal() {
        if (nullValue)
            return 0;
        long retval = value.valDateTemporal();
        if (nullValue = value.isNull())
            retval = 0;
        return retval;
    }

    @Override
    public BigDecimal valDecimal() {
        if (nullValue)
            return null;
        BigDecimal retval = value.valDecimal();
        if (nullValue = value.isNull())
            retval = null;
        return retval;
    }

    @Override
    public int getTransSize() {
        return value.getFieldLength();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        if (nullValue)
            return true;
        return (nullValue = value.getDate(ltime, fuzzydate));
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        if (nullValue)
            return true;
        return (nullValue = value.getTime(ltime));
    }

    @Override
    public String valStr() {
        if (nullValue)
            return null;
        String retval = value.valStr();
        if (nullValue = value.isNull())
            retval = null;
        return retval;
    }

    @Override
    public ItemResult resultType() {
        return hybridType;
    }

    @Override
    public FieldTypes fieldType() {
        return hybridFieldType;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        /*
         * by default it is TRUE to avoid TRUE reporting by
         * Item_func_not_all/Item_func_nop_all if this item was never called.
         *
         * no_rows_in_result() set it to FALSE if was not results found. If some
         * results found it will be left unchanged.
         */
        wasValues = true;
    }

    public boolean anyValue() {
        return wasValues;
    }

    @Override
    public void noRowsInResult() {
        wasValues = false;
        clear();
    }

}
