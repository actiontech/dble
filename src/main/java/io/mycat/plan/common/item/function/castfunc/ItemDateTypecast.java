package io.mycat.plan.common.item.function.castfunc;

import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.timefunc.ItemDateFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.util.ArrayList;
import java.util.List;

public class ItemDateTypecast extends ItemDateFunc {
    public ItemDateTypecast(Item a) {
        super(new ArrayList<Item>());
        args.add(a);
        maybeNull = true;
    }

    @Override
    public final String funcName() {
        return "cast_as_date";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzy_date) {
        boolean res = getArg0Date(ltime, fuzzy_date | MyTime.TIME_NO_DATE_FRAC_WARN);
        ltime.hour = ltime.minute = ltime.second = ltime.secondPart = 0;
        ltime.timeType = MySQLTimestampType.MYSQL_TIMESTAMP_DATE;
        return res;
    }

    @Override
    public SQLExpr toExpression() {
        SQLCastExpr cast = new SQLCastExpr();
        cast.setExpr(args.get(0).toExpression());
        SQLDataTypeImpl dataType = new SQLDataTypeImpl("DATE");
        cast.setDataType(dataType);
        return cast;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemDateTypecast(newArgs.get(0));
    }

}
