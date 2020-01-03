/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.external.ResultStore;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.field.FieldUtil;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;


public class AggregatorDistinct extends Aggregator {
    private boolean endupDone = false;
    private final ResultStore distinctRows;
    private Field field = null;

    public AggregatorDistinct(ItemSum arg, ResultStore store) {
        super(arg);
        this.distinctRows = store;
    }

    @Override
    public AggregatorType aggrType() {
        return AggregatorType.DISTINCT_AGGREGATOR;
    }

    /***************************************************************************/
    /**
     * Called before feeding the first row. Used to allocate/setup the internal
     * structures used for aggregation.
     *
     * @return status
     * @throws UnsupportedEncodingException
     * @retval FALSE success
     * @retval TRUE faliure
     * <p>
     * Prepares Aggregator_distinct to process the incoming stream.
     * Creates the temporary table and the Unique class if needed.
     * Called by Item_sum::aggregator_setup()
     */
    @Override
    public boolean setup() {
        endupDone = false;
        if (itemSum.setup())
            return true;
        // TODO see item_sum.cc for more
        FieldPacket tmp = new FieldPacket();
        itemSum.getArg(0).makeField(tmp);
        field = Field.getFieldItem(tmp.getName(), tmp.getDb(), tmp.getTable(), tmp.getOrgTable(), tmp.getType(),
                tmp.getCharsetIndex(), (int) tmp.getLength(), tmp.getDecimals(), tmp.getFlags());
        return false;
    }

    @Override
    public void clear() {
        endupDone = false;
        distinctRows.clear();
        if (!(itemSum.sumType() == ItemSum.SumFuncType.COUNT_FUNC ||
                itemSum.sumType() == ItemSum.SumFuncType.COUNT_DISTINCT_FUNC)) {
            itemSum.setNullValue(true);
        }
    }

    /**
     * add the distinct value into buffer, to use when endup() is called
     */
    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        distinctRows.add(row);
        return false;
    }

    @Override
    public void endup() {
        if (endupDone)
            return;
        itemSum.clear();
        if (distinctRows != null) {
            distinctRows.done();
            if (!endupDone) {
                useDistinctValues = true;
                RowDataPacket row = null;
                while ((row = distinctRows.next()) != null) {
                    // @bug1072 see argIsNull()
                    FieldUtil.initFields(itemSum.sourceFields, row.fieldValues);
                    field.setPtr(itemSum.getArg(0).getRowPacketByte());
                    if (itemSum.isPushDown)
                        itemSum.pushDownAdd(row);
                    else
                        itemSum.add(row, null);
                }
                useDistinctValues = false;
            }
            endupDone = true;
        }
    }

    @Override
    public BigDecimal argValDecimal() {
        return useDistinctValues ? field.valDecimal() : itemSum.getArg(0).valDecimal();
    }

    @Override
    public BigDecimal argValReal() {
        return useDistinctValues ? field.valReal() : itemSum.getArg(0).valReal();
    }

    @Override
    public boolean argIsNull() {
        return useDistinctValues ? field.isNull() : itemSum.getArg(0).isNullValue();
    }

}
