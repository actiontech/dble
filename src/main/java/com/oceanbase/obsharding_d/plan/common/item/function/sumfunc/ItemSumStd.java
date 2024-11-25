/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.sumfunc;

import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.FieldTypes;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.math.BigDecimal;
import java.util.List;


/**
 * standard_deviation(a) = sqrt(variance(a))
 */
public class ItemSumStd extends ItemSumVariance {

    public ItemSumStd(List<Item> args, int sample, boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(args, sample, isPushDown, fields, charsetIndex);
    }

    @Override
    public SumFuncType sumType() {
        return SumFuncType.STD_FUNC;
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal val = super.valReal();
        double db = Math.sqrt(val.doubleValue());
        return BigDecimal.valueOf(db);
    }

    @Override
    public String funcName() {
        return sample == 1 ? "STDDEV_SAMP" : "STD";
    }

    @Override
    public Item.ItemResult resultType() {
        return Item.ItemResult.REAL_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DOUBLE;
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        for (Item arg : args) {
            method.addArgument(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumStd(newArgs, sample, false, null, charsetIndex);
        } else {
            return new ItemSumStd(calArgs, sample, isPushDown, fields, charsetIndex);
        }
    }

}
