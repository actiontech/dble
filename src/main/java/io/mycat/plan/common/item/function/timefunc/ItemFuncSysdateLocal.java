package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

/*
 * 函数执行的时间，now函数表示的是命令接收到的的时间，在这里相同处理
 */
public class ItemFuncSysdateLocal extends ItemDatetimeFunc {

    public ItemFuncSysdateLocal(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "sysdate";
    }

    @Override
    public void fixLengthAndDec() {
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, decimals);
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        ltime.setYear(cal.get(java.util.Calendar.YEAR));
        ltime.setMonth(cal.get(java.util.Calendar.MONTH) + 1);
        ltime.setDay(cal.get(java.util.Calendar.DAY_OF_MONTH));
        ltime.setHour(cal.get(java.util.Calendar.HOUR_OF_DAY));
        ltime.setMinute(cal.get(java.util.Calendar.MINUTE));
        ltime.setSecond(cal.get(java.util.Calendar.SECOND));
        ltime.setSecondPart(cal.get(java.util.Calendar.MILLISECOND) * 1000);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSysdateLocal(realArgs);
    }
}
