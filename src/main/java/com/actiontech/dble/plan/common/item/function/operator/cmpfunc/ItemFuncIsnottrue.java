/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;

import java.util.List;


/**
 * This Item represents a <code>X IS NOT TRUE</code> boolean predicate.
 */
public class ItemFuncIsnottrue extends ItemFuncTruth {

    public ItemFuncIsnottrue(Item a, int charsetIndex) {
        super(a, true, false, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "isnottrue";
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr left = args.get(0).toExpression();
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.IsNot, new SQLBooleanExpr(true));
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncIsnottrue(newArgs.get(0), charsetIndex);
    }

}
