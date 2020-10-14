/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.castfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;


public class ItemDecimalTypeCast extends ItemFunc {
    private int precision;
    private int dec;

    public ItemDecimalTypeCast(Item a, int precision, int dec, int charsetIndex) {
        super(new ArrayList<>(), charsetIndex);
        args.add(a);
        this.precision = precision;
        this.dec = dec;
    }

    @Override
    public final String funcName() {
        return "decimal_typecast";
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
        SQLCastExpr cast = new SQLCastExpr();
        cast.setExpr(args.get(0).toExpression());
        SQLDataTypeImpl dataType = new SQLDataTypeImpl("DECIMAL");
        if (precision >= 0) {
            dataType.addArgument(new SQLIntegerExpr(precision));
        }
        if (dec > 0) {
            dataType.addArgument(new SQLIntegerExpr(dec));
        }
        cast.setDataType(dataType);
        return cast;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs;
        if (!forCalculate) {
            newArgs = cloneStructList(args);
        } else {
            newArgs = calArgs;
        }
        return new ItemDecimalTypeCast(newArgs.get(0), precision, dec, charsetIndex);
    }

}
