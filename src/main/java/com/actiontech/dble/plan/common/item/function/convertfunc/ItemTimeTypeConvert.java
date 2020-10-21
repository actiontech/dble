/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.convertfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemTimeFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.util.ArrayList;
import java.util.List;

public class ItemTimeTypeConvert extends ItemTimeFunc {

    public ItemTimeTypeConvert(Item a, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
    }

    public ItemTimeTypeConvert(Item a, int decArg, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        decimals = decArg;
    }

    @Override
    public final String funcName() {
        return "convert_as_time";
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
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        method.setMethodName("CONVERT");
        method.addParameter(args.get(0).toExpression());
        if (decimals != NOT_FIXED_DEC) {
            SQLMethodInvokeExpr dataType = new SQLMethodInvokeExpr();
            dataType.setMethodName("TIME");
            dataType.addParameter(new SQLIntegerExpr(decimals));
            method.addParameter(dataType);
        } else {
            method.addParameter(new SQLIdentifierExpr("TIME"));
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemTimeTypeConvert(newArgs.get(0), this.decimals);
    }
}
