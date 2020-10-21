/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.convertfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * CAST(expr AS type)<br>
 * type:<br>
 * BINARY[(N)]<br>
 * CHAR[(N)]<br>
 * DATE<br>
 * DATETIME<br>
 * DECIMAL[(M[,D])]<br>
 * SIGNED [INTEGER]<br>
 * TIME<br>
 * UNSIGNED [INTEGER]<br>
 *
 * @author Administrator
 */
public class ItemFuncSignedConvert extends ItemIntFunc {

    public ItemFuncSignedConvert(Item a, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
    }

    @Override
    public final String funcName() {
        return "convert_as_signed";
    }

    @Override
    public BigInteger valInt() {
        BigInteger value = BigInteger.ZERO;

        if (args.get(0).castToIntType() != ItemResult.STRING_RESULT || args.get(0).isTemporal()) {
            value = args.get(0).valInt();
            nullValue = args.get(0).isNullValue();
            return value;
        }

        try {
            value = valIntFromStr();
        } catch (Exception e) {
            value = new BigInteger("-1");
            LOGGER.info("Cast to signed converted positive out-of-range integer to " + "it's negative complement", e);
        }
        return value;
    }

    protected BigInteger valIntFromStr() throws Exception {
        /*
         * For a string result, we must first get the string and then convert it
         * to a longlong
         */

        String res = args.get(0).valStr();
        if (res == null) {
            nullValue = true;
            return BigInteger.ZERO;
        }

        nullValue = false;
        return new BigInteger(res);
    }

    @Override
    public int decimalPrecision() {
        return args.get(0).decimalPrecision();
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        method.setMethodName("CONVERT");
        method.addParameter(args.get(0).toExpression());
        method.addParameter(new SQLIdentifierExpr("SIGNED"));
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncSignedConvert(newArgs.get(0), charsetIndex);
    }
}
