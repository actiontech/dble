package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.strfunc.ItemStrFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncMonthname extends ItemStrFunc {

    public ItemFuncMonthname(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "monthname";
    }

    @Override
    public String valStr() {
        MySQLTime ltime = new MySQLTime();

        if ((nullValue = (getArg0Date(ltime, MyTime.TIME_FUZZY_DATE) || ltime.month == 0)))
            return null;
        return MyTime.MONTH_NAMES[(int) ltime.month - 1];
    }

    public void fixLengthAndDec() {
        maxLength = 9;
        decimals = 0;
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMonthname(realArgs);
    }

}
