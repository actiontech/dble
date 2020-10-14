/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc.operator;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemNumOp;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemFuncMul extends ItemNumOp {

    public ItemFuncMul(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "*";
    }

    @Override
    public BigDecimal realOp() {
        BigDecimal val0 = args.get(0).valReal();
        BigDecimal val1 = args.get(1).valReal();
        if (this.nullValue = (args.get(0).isNull() || args.get(1).isNull()))
            return BigDecimal.ZERO;
        return val0.multiply(val1);
    }

    @Override
    public BigInteger intOp() {
        BigInteger v0 = args.get(0).valInt();
        BigInteger v1 = args.get(1).valInt();
        if (this.nullValue = (args.get(0).isNull() || args.get(1).isNull()))
            return BigInteger.ZERO;
        return v0.multiply(v1);
    }

    @Override
    public BigDecimal decimalOp() {
        BigDecimal v0 = args.get(0).valDecimal();
        BigDecimal v1 = args.get(1).valDecimal();
        if (this.nullValue = (args.get(0).isNull() || args.get(1).isNull()))
            return new BigDecimal(0);
        return v0.multiply(v1);
    }

    @Override
    public void resultPrecision() {
        decimals = Math.min(args.get(0).getDecimals() + args.get(1).getDecimals(), DECIMAL_MAX_SCALE);
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLBinaryOpExpr(args.get(0).toExpression(), SQLBinaryOperator.Multiply, args.get(1).toExpression());
    }


    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncMul(newArgs.get(0), newArgs.get(1), charsetIndex);
    }
}
