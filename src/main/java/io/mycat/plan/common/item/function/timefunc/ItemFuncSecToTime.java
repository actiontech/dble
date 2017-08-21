package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.util.List;

public class ItemFuncSecToTime extends ItemTimeFunc {

    public ItemFuncSecToTime(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "sec_to_time";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, args.get(0).decimals);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        BigDecimal val = args.get(0).valDecimal();
        if (nullValue = args.get(0).nullValue) {
            return true;
        }
        long seconds = val.longValue();
        long microseconds = (long) ((val.doubleValue() - val.longValue()) * 1000000);
        if (seconds > MyTime.TIME_MAX_SECOND) {
            ltime.set_max_hhmmss();
            return true;
        }
        ltime.hour = (seconds / 3600);
        long sec = (seconds % 3600);
        ltime.minute = sec / 60;
        ltime.second = sec % 60;
        ltime.second_part = microseconds;
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSecToTime(realArgs);
    }
}
