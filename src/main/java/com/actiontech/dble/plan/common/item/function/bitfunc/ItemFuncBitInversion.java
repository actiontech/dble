/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.bitfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncBit;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryOperator;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncBitInversion extends ItemFuncBit {

    public ItemFuncBitInversion(Item a) {
        super(a);
    }

    @Override
    public final String funcName() {
        return "~";
    }

    @Override
    public BigInteger valInt() {
        BigInteger res = args.get(0).valInt();
        if (nullValue = args.get(0).isNullValue())
            return BigInteger.ZERO;
        // select ~1 18446744073709551614
        if (res.compareTo(BigInteger.ZERO) > 0) {
            return MySQLcom.BI64BACK.subtract(BigInteger.ONE).subtract(res);
        } else if (res.compareTo(BigInteger.ZERO) == 0) {
            return MySQLcom.BI64BACK.subtract(BigInteger.ONE);
        } else {
            // select ~-10; 9
            return res.negate().subtract(BigInteger.ONE);
        }
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLUnaryExpr(SQLUnaryOperator.Compl, args.get(0).toExpression());
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncBitInversion(newArgs.get(0));
    }

}
