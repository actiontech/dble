/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.convertfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemDatetimeFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.util.ArrayList;
import java.util.List;


public class ItemDatetimeTypeConvert extends ItemDatetimeFunc {
    public ItemDatetimeTypeConvert(Item a, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
    }

    public ItemDatetimeTypeConvert(Item a, int decArg, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        this.decimals = decArg;
    }

    @Override
    public final String funcName() {
        return "convert_as_datetime";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        if ((nullValue = args.get(0).getDate(ltime, fuzzyDate | MyTime.TIME_NO_DATE_FRAC_WARN)))
            return true;
        assert (ltime.getTimeType() != MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME); // In
        // case
        // it
        // was
        // DATE
        return (nullValue = MyTime.myDatetimeRound(ltime, decimals));
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        method.setMethodName("CONVERT");
        method.addParameter(args.get(0).toExpression());
        if (decimals != NOT_FIXED_DEC) {
            SQLMethodInvokeExpr dataType = new SQLMethodInvokeExpr();
            dataType.setMethodName("DATETIME");
            dataType.addParameter(new SQLIntegerExpr(decimals));
            method.addParameter(dataType);
        } else {
            method.addParameter(new SQLIdentifierExpr("DATETIME"));
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
        return new ItemDatetimeTypeConvert(newArgs.get(0), this.decimals);
    }
}
