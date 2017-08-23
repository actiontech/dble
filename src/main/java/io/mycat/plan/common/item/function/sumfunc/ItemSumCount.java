package io.mycat.plan.common.item.function.sumfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;

import java.math.BigInteger;
import java.util.List;


public class ItemSumCount extends ItemSumInt {
    long count;

    public ItemSumCount(List<Item> args, boolean distinct, boolean isPushDown, List<Field> fields) {
        super(args, isPushDown, fields);
        count = 0;
        setDistinct(distinct);
    }

    @Override
    public Sumfunctype sumType() {
        return hasWithDistinct() ? Sumfunctype.COUNT_DISTINCT_FUNC : Sumfunctype.COUNT_FUNC;
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
        } else if (!args.get(0).isNull())
            count++;
        return false;
    }

    /**
     * count(id)的pushdown为count(id)然后将他们的值进行相加
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
            return new ItemSumCount(newArgs, hasWithDistinct(), false, null);
        } else {
            return new ItemSumCount(calArgs, hasWithDistinct(), isPushDown, fields);
        }
    }

}
