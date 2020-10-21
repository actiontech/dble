/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;

import java.math.BigInteger;
import java.util.List;


public class ItemSumCount extends ItemSumInt {
    long count;

    public ItemSumCount(List<Item> args, boolean distinct, boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(args, isPushDown, fields, charsetIndex);
        count = 0;
        setDistinct(distinct);
    }

    @Override
    public SumFuncType sumType() {
        return hasWithDistinct() ? SumFuncType.COUNT_DISTINCT_FUNC : SumFuncType.COUNT_FUNC;
    }

    @Override
    public Object getTransAggObj() {
        return count;
    }

    @Override
    public int getTransSize() {
        return 10;
    }

    @Override
    public void clear() {
        count = 0;
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            long countOther = (Long) transObj;
            count += countOther;
        } else {
            for (Item arg : args) {
                if (arg.isNull()) {
                    return false;
                }
            }
            count++;
        }
        return false;
    }

    /**
     * count(id)'spush-down is count(id),and sum them
     */
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        if (!args.get(0).isNull()) {
            long val = args.get(0).valInt().longValue();
            count += val;
        }
        return false;
    }

    @Override
    public String funcName() {
        return "COUNT";
    }

    @Override
    public BigInteger valInt() {
        if (aggr != null)
            aggr.endup();
        return BigInteger.valueOf(count);
    }

    @Override
    public SQLExpr toExpression() {
        SQLAggregateExpr aggregate = new SQLAggregateExpr(funcName());
        if (hasWithDistinct()) {
            for (Item arg : args)
                aggregate.addArgument(arg.toExpression());
            aggregate.setOption(SQLAggregateOption.DISTINCT);
        } else {
            Item arg0 = getArg(0);
            aggregate.addArgument(arg0.toExpression());
        }
        return aggregate;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumCount(newArgs, hasWithDistinct(), false, null, charsetIndex);
        } else {
            return new ItemSumCount(calArgs, hasWithDistinct(), isPushDown, fields, charsetIndex);
        }
    }

}
