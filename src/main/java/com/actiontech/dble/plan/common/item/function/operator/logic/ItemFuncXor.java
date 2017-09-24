/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.logic;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncXor extends ItemBoolFunc2 {

    public ItemFuncXor(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return "xor";
    }

    @Override
    public Functype functype() {
        return Functype.XOR_FUNC;
    }

    @Override
    public BigInteger valInt() {
        int result = 0;
        nullValue = false;
        for (int i = 0; i < getArgCount(); i++) {
            result ^= (args.get(i).valInt().compareTo(BigInteger.ZERO) != 0) ? 1 : 0;
            if (args.get(i).isNullValue()) {
                nullValue = true;
                return BigInteger.ZERO;
            }
        }
        return BigInteger.valueOf(result);
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr left = args.get(0).toExpression();
        SQLExpr right = args.get(1).toExpression();
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.BooleanXor, right);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncXor(newArgs.get(0), newArgs.get(1));
    }

}
