/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.convertfunc;

import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.strfunc.ItemStrFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.util.ArrayList;
import java.util.List;


public class ItemFuncBinaryConvert extends ItemStrFunc {
    private int castLength;

    public ItemFuncBinaryConvert(Item a, int lengthArg, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        this.castLength = lengthArg;
    }

    @Override
    public final String funcName() {
        return "convert_as_binary";
    }

    @Override
    public String valStr() {
        assert (fixed);
        String res;
        if ((res = args.get(0).valStr()) == null) {
            nullValue = true;
            return null;
        }
        nullValue = false;
        if (castLength != -1 && castLength < res.length())
            res = res.substring(0, castLength);
        return res;
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(castLength >= 0 ? castLength : args.get(0).getMaxLength());
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        method.setMethodName("CONVERT");
        method.addArgument(args.get(0).toExpression());
        if (decimals != NOT_FIXED_DEC) {
            SQLMethodInvokeExpr dataType = new SQLMethodInvokeExpr();
            dataType.setMethodName("BINARY");
            dataType.addArgument(new SQLIntegerExpr(decimals));
            method.addArgument(dataType);
        } else {
            method.addArgument(new SQLIdentifierExpr("BINARY"));
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
        return new ItemFuncBinaryConvert(newArgs.get(0), castLength, charsetIndex);
    }
}
