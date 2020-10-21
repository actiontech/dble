/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc.operator;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncNum1;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryOperator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public class ItemFuncNeg extends ItemFuncNum1 {

    public ItemFuncNeg(Item a, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
    }

    @Override
    public final String funcName() {
        return "-";
    }

    @Override
    public BigInteger intOp() {
        BigInteger bi = args.get(0).valInt();
        if (nullValue = args.get(0).isNull()) {
            return BigInteger.ZERO;
        }
        return bi.negate();
    }

    @Override
    public BigDecimal realOp() {
        BigDecimal bd = args.get(0).valReal();
        nullValue = args.get(0).isNull();
        return bd.negate();
    }

    @Override
    public BigDecimal decimalOp() {
        BigDecimal bd = args.get(0).valDecimal();
        if (nullValue = args.get(0).isNullValue())
            return BigDecimal.ZERO;
        return bd.negate();
    }

    @Override
    public void fixNumLengthAndDec() {
        decimals = args.get(0).getDecimals();
    }

    @Override
    public int decimalPrecision() {
        return args.get(0).decimalPrecision();
    }

    @Override
    public Functype functype() {
        return Functype.NEG_FUNC;
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLUnaryExpr(SQLUnaryOperator.Negative, args.get(0).toExpression());
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncNeg(newArgs.get(0), charsetIndex);
    }
}
