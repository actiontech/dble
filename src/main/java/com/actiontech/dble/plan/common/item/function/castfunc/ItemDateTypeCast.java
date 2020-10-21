/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.castfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemDateFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;

import java.util.ArrayList;
import java.util.List;

public class ItemDateTypeCast extends ItemDateFunc {
    public ItemDateTypeCast(Item a, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        maybeNull = true;
    }

    @Override
    public final String funcName() {
        return "cast_as_date";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        final boolean res = getArg0Date(ltime, fuzzyDate | MyTime.TIME_NO_DATE_FRAC_WARN);
        ltime.setSecondPart(0);
        ltime.setSecond(0);
        ltime.setMinute(0);
        ltime.setHour(0);
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);
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
        return new ItemDateTypeCast(newArgs.get(0), charsetIndex);
    }

}
