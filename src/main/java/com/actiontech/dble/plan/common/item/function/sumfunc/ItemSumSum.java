/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;


public class ItemSumSum extends ItemSumNum {

    public ItemSumSum(List<Item> args, boolean distinct, boolean isPushDown, List<Field> fields) {
        super(args, isPushDown, fields);
        setDistinct(distinct);
    }

    protected ItemResult hybridType;
    protected BigDecimal sum;

    @Override
    public void fixLengthAndDec() {
        maybeNull = nullValue = true;
        decimals = args.get(0).getDecimals();
        ItemResult i = args.get(0).numericContextResultType();
        if (i == ItemResult.REAL_RESULT) {
            hybridType = ItemResult.REAL_RESULT;
            sum = BigDecimal.ZERO;

        } else if (i == ItemResult.INT_RESULT || i == ItemResult.DECIMAL_RESULT) {
            int precision = args.get(0).decimalPrecision() + MySQLcom.DECIMAL_LONGLONG_DIGITS;
            maxLength = precision + 2; // 2 means Decimal point and Minus .
            //hybridType = ItemResult.DECIMAL_RESULT;
            //sum = BigDecimal.ZERO;
        } else {
            assert (false);
        }
    }

    public Sumfunctype sumType() {
        return hasWithDistinct() ? Sumfunctype.SUM_DISTINCT_FUNC : Sumfunctype.SUM_FUNC;
    }

    @Override
    public void clear() {
        nullValue = true;
        sum = BigDecimal.ZERO;
    }

    @Override
    public Object getTransAggObj() {
        AggData data = new AggData(sum, nullValue);
        return data;
    }

    @Override
    public int getTransSize() {
        return 10;
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            AggData data = (AggData) transObj;
            if (hybridType == ItemResult.DECIMAL_RESULT) {
                final BigDecimal val = data.bd;
                if (!data.isNull) {
                    sum = sum.add(val);
                    nullValue = false;
                }
            } else {
                sum = sum.add(data.bd);
                if (!data.isNull)
                    nullValue = false;
            }
        } else {
            if (hybridType == ItemResult.DECIMAL_RESULT) {
                final BigDecimal val = aggr.argValDecimal();
                if (!aggr.argIsNull()) {
                    sum = sum.add(val);
                    nullValue = false;
                }
            } else {
                sum = sum.add(aggr.argValReal());
                if (!aggr.argIsNull())
                    nullValue = false;
            }
        }
        return false;
    }

    /**
     * sum(id)'s push down issum(id)
     */
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        if (hybridType == ItemResult.DECIMAL_RESULT) {
            final BigDecimal val = aggr.argValDecimal();
            if (!aggr.argIsNull()) {
                sum = sum.add(val);
                nullValue = false;
            }
        } else {
            sum = sum.add(aggr.argValReal());
            if (!aggr.argIsNull())
                nullValue = false;
        }
        return false;
    }

    @Override
    public BigDecimal valReal() {
        if (aggr != null) {
            aggr.endup();
        }
        return sum;
    }

    @Override
    public ItemResult resultType() {
        return hybridType;
    }

    @Override
    public String funcName() {
        return "SUM";
    }

    @Override
    public SQLExpr toExpression() {
        Item arg0 = getArg(0);
        SQLAggregateExpr aggregate = new SQLAggregateExpr(funcName());
        aggregate.addArgument(arg0.toExpression());
        if (hasWithDistinct()) {
            aggregate.setOption(SQLAggregateOption.DISTINCT);
        }
        return aggregate;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumSum(newArgs, hasWithDistinct(), false, null);
        } else {
            return new ItemSumSum(calArgs, hasWithDistinct(), isPushDown, fields);
        }
    }

    protected static class AggData implements Serializable {

        private static final long serialVersionUID = 6951860386146676307L;

        private BigDecimal bd;
        protected boolean isNull;

        public AggData(BigDecimal bd, boolean isNull) {
            this.bd = bd;
            this.isNull = isNull;
        }

    }
}
