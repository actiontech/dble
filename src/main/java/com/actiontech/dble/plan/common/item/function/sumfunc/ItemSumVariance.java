/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.DoublePtr;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;


/**
 * /* variance(a) =
 * <p>
 * = sum (ai - avg(a))^2 / count(a) ) = sum (ai^2 - 2*ai*avg(a) + avg(a)^2) /
 * count(a) = (sum(ai^2) - sum(2*ai*avg(a)) + sum(avg(a)^2))/count(a) = =
 * (sum(ai^2) - 2*avg(a)*sum(a) + count(a)*avg(a)^2)/count(a) = = (sum(ai^2) -
 * 2*sum(a)*sum(a)/count(a) + count(a)*sum(a)^2/count(a)^2 )/count(a) = =
 * (sum(ai^2) - 2*sum(a)^2/count(a) + sum(a)^2/count(a) )/count(a) = =
 * (sum(ai^2) - sum(a)^2/count(a))/count(a)
 * <p>
 * But, this falls prey to catastrophic cancellation. Instead, use the
 * recurrence formulas
 * <p>
 * M_{1} = x_{1}, ~ M_{k} = M_{k-1} + (x_{k} - M_{k-1}) / k S_{1} = 0, ~S_{k} =
 * S_{k-1} + (x_{k} - M_{k-1}) times (x_{k} - M_{k}) for 2 <= k <= n ital
 * variance = S_{n} / (n-1)
 *
 * @author ActionTech
 */

public class ItemSumVariance extends ItemSumNum {
    protected ItemResult hybridType;
    protected double recurrenceM, recurrenceS; /* Used in recurrence relation. */
    protected long count = 0;
    protected int sample;

    // pushdown variables
    // v[0]:count,v[1]:sum,v[2]:variance(Partial)
    // variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
    private double sum = 0;
    private double squareSum = 0;

    private boolean useTransObj = false;
    // variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
    private double sumAi2 = 0;
    private double sumA = 0;

    @Override
    public void fixLengthAndDec() {
        maybeNull = nullValue = true;
        hybridType = ItemResult.REAL_RESULT;
        decimals = NOT_FIXED_DEC;
        maxLength = floatLength(decimals);
    }

    @Override
    public SumFuncType sumType() {
        return SumFuncType.VARIANCE_FUNC;
    }

    @Override
    public void clear() {
        count = 0;
        sum = squareSum = 0.0;
        sumA = sumAi2 = 0.0;
        useTransObj = false;
    }

    @Override
    public Object getTransAggObj() {
        AggData data = new AggData(sumAi2, sumA, count);
        return data;
    }

    @Override
    public int getTransSize() {
        return 20;
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            useTransObj = true;
            AggData other = (AggData) transObj;
            sumAi2 += other.sumAi2;
            sumA += other.sumA;
            count += other.count;
        } else {
            /*
             * Why use a temporary variable? We don't know if it is null until
             * we evaluate it, which has the side-effect of setting null_value .
             */
            double nr = args.get(0).valReal().doubleValue();
            // add for transObj
            sumA += nr;
            sumAi2 += nr * nr;
            // end add
            if (!args.get(0).isNullValue()) {
                DoublePtr rM = new DoublePtr(recurrenceM);
                DoublePtr rS = new DoublePtr(recurrenceS);
                LongPtr countPtr = new LongPtr(count);
                varianceFpRecurrenceNext(rM, rS, countPtr, nr);
                recurrenceM = rM.get();
                recurrenceS = rS.get();
                count = countPtr.get();
            }
        }
        return false;
    }

    // pushdown variables
    // push down v[0]:count,v[1]:sum,v[2]:variance(Partial)
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        //  variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
        long partCount = args.get(0).valInt().longValue();
        double partSum = args.get(1).valReal().doubleValue();
        double partVariane = args.get(2).valReal().doubleValue();
        if (partCount != 0) {
            count += partCount;
            double partSqarSum = partVariane * partCount + partSum * partSum / partCount;
            squareSum += partSqarSum;
            sum += partSum;
        }

        return false;
    }

    // variance = (sum(ai^2) - sum(a)^2/count(a))/count(a)
    @Override
    public BigDecimal valReal() {
        if (!isPushDown) {
            /*
             * 'sample' is a 1/0 boolean value. If it is 1/true, id est this is
             * a sample variance call, then we should set nullness when the
             * count of the items is one or zero. If it's zero, i.e. a
             * population variance, then we only set nullness when the count is
             * zero.
             *
             * Another way to read it is that 'sample' is the numerical
             * threshhold, at and below which a 'count' number of items is
             * called NULL.
             */
            assert ((sample == 0) || (sample == 1));
            if (count <= sample) {
                nullValue = true;
                return BigDecimal.ZERO;
            }

            nullValue = false;
            if (!useTransObj) {
                double db = varianceFpRecurrenceResult(recurrenceS, count, sample != 0);
                return BigDecimal.valueOf(db);
            } else {
                double db = (sumAi2 - sumA * sumA / count) / (count - sample);
                return BigDecimal.valueOf(db);
            }
        } else {
            double db = pushDownVal();
            return BigDecimal.valueOf(db);
        }
    }

    private double pushDownVal() {
        if (count <= sample) {
            nullValue = true;
            return 0.0;
        }
        nullValue = false;
        if (count == 1)
            return 0.0;

        double s = (squareSum - sum * sum / count);

        if (sample == 1)
            return s / (count - 1);
        else
            return s / count;

    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromReal();
    }

    @Override
    public void noRowsInResult() {
    }

    @Override
    public String funcName() {
        return sample == 1 ? "VAR_SAMP" : "VARIANCE";
    }

    @Override
    public void cleanup() {
        count = 0;
        super.cleanup();
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.REAL_RESULT;
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        for (Item arg : args) {
            method.addParameter(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumVariance(newArgs, sample, false, null, charsetIndex);
        } else {
            return new ItemSumVariance(calArgs, sample, isPushDown, fields, charsetIndex);
        }
    }

    public ItemSumVariance(List<Item> args, int sample, boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(args, isPushDown, fields, charsetIndex);
        this.sample = sample;
    }

    private static class AggData implements Serializable {

        private static final long serialVersionUID = -5441804522036055390L;

        private double sumAi2;
        private double sumA;
        private long count;

        AggData(double sumAi2, double sumA, long count) {
            this.sumAi2 = sumAi2;
            this.sumA = sumA;
            this.count = count;
        }

    }
}
