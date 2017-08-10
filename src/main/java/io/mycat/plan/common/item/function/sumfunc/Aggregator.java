package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigDecimal;

import io.mycat.net.mysql.RowDataPacket;


public abstract class Aggregator {

	/*
	 * All members are protected as this class is not usable outside of an
	 * Item_sum descendant.
	 */
	/* the aggregate function class to act on */
	protected ItemSum item_sum;

	/**
	 * When feeding back the data in endup() from Unique/temp table back to
	 * Item_sum::add(List<byte[]> rowBytes) methods we must read the data from
	 * Unique (and not recalculate the functions that are given as arguments to
	 * the aggregate function. This flag is to tell the add(List
	 * <byte[]> rowBytes) methods to take the data from the Unique instead by
	 * calling the relevant val_..() method
	 */
	protected boolean use_distinct_values;

	public Aggregator(ItemSum arg) {
		item_sum = (arg);
		use_distinct_values = (false);
	}

	public enum AggregatorType {
		SIMPLE_AGGREGATOR, DISTINCT_AGGREGATOR
	};

	public abstract AggregatorType aggrType();

	/**
	 * Called before adding the first row. Allocates and sets up the internal
	 * aggregation structures used, e.g. the Unique instance used to calculate
	 * distinct.
	 */
	public abstract boolean setup();

	/**
	 * Called when we need to wipe out all the data from the aggregator : all
	 * the values acumulated and all the state. Cleans up the internal
	 * structures and resets them to their initial state.
	 */
	public abstract void clear();

	/**
	 * Called when there's a new value to be aggregated. Updates the internal
	 * state of the aggregator to reflect the new value.
	 */
	public abstract boolean add(RowDataPacket row, Object transObj);

	/**
	 * Called when there are no more data and the final value is to be
	 * retrieved. Finalises the state of the aggregator, so the final result can
	 * be retrieved.
	 */
	public abstract void endup();

	/** Decimal value of being-aggregated argument */
	public abstract BigDecimal arg_val_decimal();

	/** Floating point value of being-aggregated argument */
	public abstract BigDecimal arg_val_real();

	/**
	 * NULLness of being-aggregated argument; can be called only after
	 * arg_val_decimal() or arg_val_real().
	 */
	public abstract boolean arg_is_null();
}
