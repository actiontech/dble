/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;

import java.math.BigDecimal;


public abstract class Aggregator {

    /*
     * All members are protected as this class is not usable outside of an
     * Item_sum descendant.
     */
    /* the aggregate function class to act on */
    protected ItemSum itemSum;

    /**
     * When feeding back the data in endup() from Unique/temp table back to
     * Item_sum::add(List<byte[]> rowBytes) methods we must read the data from
     * Unique (and not recalculate the functions that are given as arguments to
     * the aggregate function. This flag is to tell the add(List
     * <byte[]> rowBytes) methods to take the data from the Unique instead by
     * calling the relevant val_..() method
     */
    protected boolean useDistinctValues;

    public Aggregator(ItemSum arg) {
        itemSum = (arg);
        useDistinctValues = (false);
    }

    public enum AggregatorType {
        SIMPLE_AGGREGATOR, DISTINCT_AGGREGATOR
    }

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

    /**
     * Decimal value of being-aggregated argument
     */
    public abstract BigDecimal argValDecimal();

    /**
     * Floating point value of being-aggregated argument
     */
    public abstract BigDecimal argValReal();

    /**
     * NULLness of being-aggregated argument; can be called only after
     * argValDecimal() or argValReal().
     */
    public abstract boolean argIsNull();
}
