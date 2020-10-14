/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc.operator;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public class ItemFuncIntDiv extends ItemIntFunc {

    public ItemFuncIntDiv(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "div";
    }

    @Override
    public BigInteger valInt() {
        /*
         * Perform division using DECIMAL math if either of the operands has a
         * non-integer type
         */
        if (args.get(0).resultType() != ItemResult.INT_RESULT ||
                args.get(1).resultType() != ItemResult.INT_RESULT) {
            BigDecimal val0p = args.get(0).valDecimal();
            if (args.get(0).isNull())
                return BigInteger.ZERO;
            BigDecimal val1p = args.get(1).valDecimal();
            if (args.get(1).isNull())
                return BigInteger.ZERO;
            try {
                BigDecimal result = val0p.divideToIntegralValue(val1p);
                return result.toBigInteger();
            } catch (ArithmeticException ae) {
                return BigInteger.ZERO;
            }
        } else {
            BigInteger val0 = args.get(0).valInt();
            BigInteger val1 = args.get(1).valInt();
            if (args.get(0).isNull() || args.get(1).isNull())
                return BigInteger.ZERO;
            if (val1.equals(BigInteger.ZERO)) {
                signalDivideByNull();
                return BigInteger.ZERO;
            }
            return val0.divide(val1);
        }
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLBinaryOpExpr(args.get(0).toExpression(), SQLBinaryOperator.DIV, args.get(1).toExpression());
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncIntDiv(newArgs.get(0), newArgs.get(1), charsetIndex);
    }

}
