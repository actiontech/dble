/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.FieldUtil;

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
    public BigDecimal argValDecimal() {
        return itemSum.args.get(0).valDecimal();
    }

    @Override
    public BigDecimal argValReal() {
        return itemSum.args.get(0).valReal();
    }

    @Override
    public boolean argIsNull() {
        return itemSum.args.get(0).isNullValue();
    }

}
