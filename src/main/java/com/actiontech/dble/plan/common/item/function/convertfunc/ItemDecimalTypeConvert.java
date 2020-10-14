/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.convertfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;


public class ItemDecimalTypeConvert extends ItemFunc {
    private int precision;
    private int dec;

    public ItemDecimalTypeConvert(Item a, int precision, int dec, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        this.precision = precision;
        this.dec = dec;
    }

    @Override
    public final String funcName() {
        return "convert_decimal";
    }

    @Override
    public void fixLengthAndDec() {
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal tmp = valDecimal();
        if (nullValue)
            return BigDecimal.ZERO;
        return tmp;
    }

    @Override
    public BigInteger valInt() {
        BigDecimal tmp = valDecimal();
        if (nullValue)
            return BigInteger.ZERO;
        return tmp.toBigInteger();
    }

    @Override
    public String valStr() {
        BigDecimal tmp = valDecimal();
        if (nullValue)
            return null;
        return tmp.toString();
    }

    @Override
    public BigDecimal valDecimal() {
        BigDecimal tmp = args.get(0).valDecimal();

        if ((nullValue = args.get(0).isNullValue()))
            return null;
        return tmp.setScale(this.dec, RoundingMode.HALF_UP);
    }

    @Override
    public boolean getDate(MySQLTime ltime, long flags) {
        return getDateFromDecimal(ltime, flags);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromDecimal(ltime);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DECIMAL;
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.DECIMAL_RESULT;
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        method.setMethodName("CONVERT");
        method.addParameter(args.get(0).toExpression());
        if (precision >= 0 || dec > 0) {
            SQLMethodInvokeExpr dataType = new SQLMethodInvokeExpr();
            dataType.setMethodName("DECIMAL");
            if (precision >= 0) {
                dataType.addParameter(new SQLIntegerExpr(precision));
            }
            if (dec > 0) {
                dataType.addParameter(new SQLIntegerExpr(dec));
            }
            method.addParameter(dataType);
        } else {
            method.addParameter(new SQLIdentifierExpr("DECIMAL"));
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs;
        if (!forCalculate) {
            newArgs = cloneStructList(args);
        } else {
            newArgs = calArgs;
        }
        return new ItemDecimalTypeConvert(newArgs.get(0), precision, dec, charsetIndex);
    }

}
