package io.mycat.plan.common.item.function.sumfunc;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;


public abstract class ItemSumBit extends ItemSumInt {

    protected BigInteger resetBits, bits;

    public ItemSumBit(List<Item> item_par, long reset_arg, boolean isPushDown, List<Field> fields) {
        super(item_par, isPushDown, fields);
        resetBits = BigInteger.valueOf(reset_arg);
        bits = BigInteger.valueOf(reset_arg);
    }

    public Sumfunctype sumType() {
        return Sumfunctype.SUM_BIT_FUNC;
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

    protected static class AggData implements Serializable {

        private static final long serialVersionUID = -5952130248997591472L;

        public BigInteger bits;
        public boolean isNull;

        public AggData(BigInteger bits, boolean isNull) {
            this.bits = bits;
            this.isNull = isNull;
        }

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

}
