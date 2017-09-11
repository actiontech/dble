/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.castfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemTimeFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import java.util.ArrayList;
import java.util.List;

public class ItemTimeTypecast extends ItemTimeFunc {

    public ItemTimeTypecast(Item a) {
        super(new ArrayList<Item>());
        args.add(a);
    }

    public ItemTimeTypecast(Item a, int decArg) {
        super(new ArrayList<Item>());
        args.add(a);
        decimals = decArg;
    }

    @Override
    public final String funcName() {
        return "cast_as_time";
    }

    public boolean getTime(MySQLTime ltime) {
        if (getArg0Time(ltime))
            return true;
        if (decimals != NOT_FIXED_DEC) {
            MyTime.myTimeRound(ltime, decimals);
        }
        /*
         * For MYSQL_TIMESTAMP_TIME value we can have non-zero day part, which
         * we should not lose.
         */
        if (ltime.getTimeType() != MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
            MyTime.datetimeToTime(ltime);
        return false;
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
    }

    @Override
    public SQLExpr toExpression() {
        SQLCastExpr cast = new SQLCastExpr();
        cast.setExpr(args.get(0).toExpression());
        SQLDataTypeImpl dataType = new SQLDataTypeImpl("TIME");
        if (decimals != NOT_FIXED_DEC) {
            dataType.addArgument(new SQLIntegerExpr(decimals));
        }
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
        return new ItemTimeTypecast(newArgs.get(0), this.decimals);
    }
}
