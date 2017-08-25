package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MySQLTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncMinute extends ItemIntFunc {

    public ItemFuncMinute(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "minute";
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime = new MySQLTime();
        return getArg0Time(ltime) ? BigInteger.ZERO : BigInteger.valueOf(ltime.getMinute());
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = (2); /* 0..59 */
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMinute(realArgs);
    }
}
