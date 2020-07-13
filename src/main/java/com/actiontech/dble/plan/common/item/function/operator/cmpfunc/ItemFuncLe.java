/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncLe extends ItemBoolFunc2 {

    public ItemFuncLe(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "<=";
    }

    @Override
    public ItemFunc.Functype functype() {
        return ItemFunc.Functype.LE_FUNC;
    }

    @Override
    public BigInteger valInt() {
        int value = cmp.compare();
        return value <= 0 && !nullValue ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr left = args.get(0).toExpression();
        SQLExpr right = args.get(1).toExpression();
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.LessThanOrEqual, right);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncLe(newArgs.get(0), newArgs.get(1), this.charsetIndex);
    }

}
