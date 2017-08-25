package io.mycat.plan.common.item.function.sumfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;


public class ItemSumAvg extends ItemSumSum {
    private long count;

    public ItemSumAvg(List<Item> args, boolean distinct, boolean isPushDown, List<Field> fields) {
        super(args, distinct, isPushDown, fields);
        count = 0;
    }

    @Override
    public void fixLengthAndDec() {
        super.fixLengthAndDec();
        maybeNull = nullValue = true;
    }

    @Override
    public Sumfunctype sumType() {
        return hasWithDistinct() ? Sumfunctype.AVG_DISTINCT_FUNC : Sumfunctype.AVG_FUNC;
    }

    @Override
    public void clear() {
        super.clear();
        count = 0;
    }

    @Override
    public Object getTransAggObj() {
        AvgAggData aggData = new AvgAggData(sum, count, nullValue);
        return aggData;
    }

    @Override
    public int getTransSize() {
        return 20;
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            AvgAggData data = (AvgAggData) transObj;
            if (super.add(row, data))
                return true;
            if (!data.isNull)
                count += data.count;
        } else {
            if (super.add(row, null))
                return true;
            if (!aggr.argIsNull())
                count++;
        }
        return false;
    }

    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        // 聚合函数下发的情况，avg(n)被下发成sum(n) 和 count(n);
        assert (getArgCount() == 2);
        count += args.get(1).valInt().longValue();
        return super.add(row, null);
    }

    @Override
    public BigDecimal valReal() {
        if (aggr != null)
            aggr.endup();
        if (count == 0) {
            nullValue = true;
            return BigDecimal.ZERO;
        }

        return super.valReal().divide(new BigDecimal(count), decimals + 4, RoundingMode.HALF_UP);
    }

    @Override
    public BigInteger valInt() {
        return valReal().toBigInteger();
    }

    @Override
    public BigDecimal valDecimal() {
        if (aggr != null)
            aggr.endup();
        if (count == 0) {
            nullValue = true;
            return null;
        }
        return valReal();
    }

    @Override
    public String valStr() {
        if (aggr != null)
            aggr.endup();
        if (hybridType == ItemResult.DECIMAL_RESULT)
            return valStringFromDecimal();
        return valStringFromReal();
    }

    @Override
    public void cleanup() {
        count = 0;
        super.cleanup();
    }

    @Override
    public final String funcName() {
        return "AVG";
    }

    @Override
    public void noRowsInResult() {
    }

    @Override
    public SQLExpr toExpression() {
        Item arg0 = args.get(0);
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
            return new ItemSumAvg(newArgs, hasWithDistinct(), false, null);
        } else {
            return new ItemSumAvg(calArgs, hasWithDistinct(), isPushDown, fields);
        }
    }

    private static class AvgAggData extends AggData {

        private static final long serialVersionUID = -1831762635995954526L;
        private long count;

        AvgAggData(BigDecimal sum, long count, boolean isNull) {
            super(sum, isNull);
            this.count = count;
        }

    }
}
