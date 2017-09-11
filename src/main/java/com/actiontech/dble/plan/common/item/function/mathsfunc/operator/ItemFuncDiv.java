/*
 * Copyright (C) 2016-2017 ActionTech.
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
import java.math.RoundingMode;
import java.util.List;


public class ItemFuncDiv extends ItemNumOp {

    /**
     * default size of bigdecimal, the correct way is get the conf of mysql
     */
    private int precIncrement = 4;

    public ItemFuncDiv(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return "/";
    }

    @Override
    public void fixLengthAndDec() {
        super.fixLengthAndDec();
        if (hybridType == ItemResult.REAL_RESULT) {
            decimals = Math.max(args.get(0).getDecimals(), args.get(1).getDecimals()) + precIncrement;
            decimals = Math.min(decimals, NOT_FIXED_DEC);
            int tmp = floatLength(decimals);
            if (decimals == NOT_FIXED_DEC)
                maxLength = tmp;
            else {
                maxLength = args.get(0).getMaxLength() - args.get(1).getDecimals() + decimals;
                maxLength = Math.min(maxLength, tmp);
            }
        } else if (hybridType == ItemResult.INT_RESULT) {
            hybridType = ItemResult.DECIMAL_RESULT;
            resultPrecision();

        } else if (hybridType == ItemResult.DECIMAL_RESULT) {
            resultPrecision();


        }
    }

    @Override
    public BigDecimal realOp() {
        BigDecimal val0 = args.get(0).valReal();
        BigDecimal val1 = args.get(1).valReal();
        if ((this.nullValue = args.get(0).isNull() || args.get(1).isNull()))
            return BigDecimal.ZERO;
        if (val1.compareTo(BigDecimal.ZERO) == 0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        return val0.divide(val1, decimals, RoundingMode.HALF_UP);
    }

    @Override
    public BigInteger intOp() {
        assert (false);
        return BigInteger.ZERO;
    }

    @Override
    public BigDecimal decimalOp() {
        if ((this.nullValue = args.get(0).isNull()))
            return new BigDecimal(0);
        if ((this.nullValue = args.get(1).isNull()))
            return new BigDecimal(0);

        final BigDecimal val2 = args.get(1).valDecimal();
        if (val2.compareTo(BigDecimal.ZERO) == 0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        final BigDecimal val1 = args.get(0).valDecimal();
        BigDecimal bd = val1.divide(val2, decimals, RoundingMode.HALF_UP);
        return bd;
    }

    @Override
    public void resultPrecision() {
        decimals = Math.min(args.get(0).getDecimals() + precIncrement, DECIMAL_MAX_SCALE);
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLBinaryOpExpr(args.get(0).toExpression(), SQLBinaryOperator.Divide, args.get(1).toExpression());
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncDiv(newArgs.get(0), newArgs.get(1));
    }

}
