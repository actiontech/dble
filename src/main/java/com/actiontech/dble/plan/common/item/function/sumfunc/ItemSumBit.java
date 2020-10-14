/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;


public abstract class ItemSumBit extends ItemSumInt {

    protected BigInteger resetBits, bits;

    public ItemSumBit(List<Item> itemPar, long resetArg, boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(itemPar, isPushDown, fields, charsetIndex);
        resetBits = BigInteger.valueOf(resetArg);
        bits = BigInteger.valueOf(resetArg);
    }

    public SumFuncType sumType() {
        return SumFuncType.SUM_BIT_FUNC;
    }

    @Override
    public void clear() {
        bits = resetBits;
    }

    @Override
    public BigInteger valInt() {
        return bits;
    }

    @Override
    public void fixLengthAndDec() {
        decimals = 0;
        maxLength = 21;
        maybeNull = nullValue = false;
    }

    @Override
    public void cleanup() {
        bits = resetBits;
        super.cleanup();
    }

    @Override
    public Object getTransAggObj() {
        AggData data = new AggData(bits, nullValue);
        return data;
    }

    @Override
    public int getTransSize() {
        return 15;
    }

    protected static class AggData implements Serializable {

        private static final long serialVersionUID = -5952130248997591472L;

        protected BigInteger bits;
        protected boolean isNull;

        public AggData(BigInteger bits, boolean isNull) {
            this.bits = bits;
            this.isNull = isNull;
        }

    }

}
