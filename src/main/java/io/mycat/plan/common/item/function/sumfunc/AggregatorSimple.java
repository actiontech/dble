package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigDecimal;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.FieldUtil;


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
		return item_sum.setup();
	}

	@Override
	public void clear() {
		item_sum.clear();
	}

	@Override
	public boolean add(RowDataPacket row, Object transObj) {
		FieldUtil.initFields(item_sum.sourceFields, row.fieldValues);
		if (!item_sum.isPushDown)
			return item_sum.add(row, transObj);
		else
			return item_sum.pushDownAdd(row);
	}

	@Override
	public void endup() {
	}

	@Override
	public BigDecimal arg_val_decimal() {
		return item_sum.args.get(0).valDecimal();
	}

	@Override
	public BigDecimal arg_val_real() {
		return item_sum.args.get(0).valReal();
	}

	@Override
	public boolean arg_is_null() {
		return item_sum.args.get(0).nullValue;
	}

}
