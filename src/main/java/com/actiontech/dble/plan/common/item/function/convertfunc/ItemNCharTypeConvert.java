/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.convertfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.util.ArrayList;
import java.util.List;


public class ItemNCharTypeConvert extends ItemStrFunc {
    private int castLength;

    public ItemNCharTypeConvert(Item a, int lengthArg, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        this.castLength = lengthArg;
    }

    @Override
    public final String funcName() {
        return "convert_as_nchar";
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(castLength >= 0 ? castLength : args.get(0).getMaxLength());
    }

    @Override
    public String valStr() {
        assert (fixed && castLength >= 0);

        String res = null;
        if ((res = args.get(0).valStr()) == null) {
            nullValue = true;
            return null;
        }
        nullValue = false;
        if (castLength < res.length())
            res = res.substring(0, castLength);
        return res;
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        method.setMethodName("CONVERT");
        method.addParameter(args.get(0).toExpression());
        if (castLength >= 0) {
            SQLMethodInvokeExpr dataType = new SQLMethodInvokeExpr();
            dataType.setMethodName("NCHAR");
            dataType.addParameter(new SQLIntegerExpr(castLength));
            method.addParameter(dataType);
        } else {
            method.addParameter(new SQLIdentifierExpr("NCHAR"));
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
        return new ItemNCharTypeConvert(newArgs.get(0), castLength, charsetIndex);
    }
}
