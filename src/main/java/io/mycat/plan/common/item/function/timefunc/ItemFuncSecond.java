package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MySQLTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncSecond extends ItemIntFunc {

    public ItemFuncSecond(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "second";
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime = new MySQLTime();
        return getArg0Time(ltime) ? BigInteger.ZERO : BigInteger.valueOf(ltime.getSecond());
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(2); /* 0..59 */
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSecond(realArgs);
    }
}
