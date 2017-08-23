package io.mycat.plan.common.item.function.sumfunc;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.FieldUtil;

import java.math.BigDecimal;


public class AggregatorSimple extends Aggregator {

    public AggregatorSimple(ItemSum arg) {
        super(arg);
    }

    @Override
    public AggregatorType aggrType() {
        return AggregatorType.SIMPLE_AGGREGATOR;
    }

    @Override
    public boolean setup() {
        return itemSum.setup();
    }

    @Override
    public void clear() {
        itemSum.clear();
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        FieldUtil.initFields(itemSum.sourceFields, row.fieldValues);
        if (!itemSum.isPushDown)
            return itemSum.add(row, transObj);
        else
            return itemSum.pushDownAdd(row);
    }

    @Override
    public void endup() {
    }

    @Override
    public BigDecimal arg_val_decimal() {
        return itemSum.args.get(0).valDecimal();
    }

    @Override
    public BigDecimal arg_val_real() {
        return itemSum.args.get(0).valReal();
    }

    @Override
    public boolean arg_is_null() {
        return itemSum.args.get(0).nullValue;
    }

}
