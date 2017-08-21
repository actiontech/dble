package io.mycat.plan.common.item.function.bitfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


/**
 * 参数是任意类型 <br>
 * 返回值是BIG INT
 *
 * @author Administrator
 */
public class ItemFuncBitCount extends ItemIntFunc {

    public ItemFuncBitCount(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "bit_count";
    }

    @Override
    public BigInteger valInt() {
        BigInteger value = args.get(0).valInt();
        if ((nullValue = args.get(0).nullValue))
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(value.bitCount());
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 2;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncBitCount(realArgs);
    }
}
