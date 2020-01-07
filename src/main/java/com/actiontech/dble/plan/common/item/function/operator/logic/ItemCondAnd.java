/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.logic;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.math.BigInteger;
import java.util.List;


public class ItemCondAnd extends ItemCond {

    public ItemCondAnd(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "and";
    }

    @Override
    public Functype functype() {
        return Functype.COND_AND_FUNC;
    }

    @Override
    public BigInteger valInt() {
        nullValue = false;
        for (Item item : list) {
            if (!item.valBool()) {
                if (!(nullValue = item.isNullValue()))
                    return BigInteger.ZERO; // return FALSE
            }
        }
        return nullValue ? BigInteger.ZERO : BigInteger.ONE;
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr left = args.get(0).toExpression();
        SQLExpr right = args.get(1).toExpression();
        SQLExpr result = new SQLBinaryOpExpr(left, SQLBinaryOperator.BooleanAnd, right);
        for (int i = 2; i < args.size(); i++) {
            SQLExpr rightAnother = args.get(i).toExpression();
            result = new SQLBinaryOpExpr(result, SQLBinaryOperator.BooleanAnd, rightAnother);
        }
        return result;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemCondAnd(newArgs);
    }

}
