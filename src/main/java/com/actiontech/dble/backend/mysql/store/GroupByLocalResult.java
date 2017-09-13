/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.diskbuffer.GroupResultDiskBuffer;
import com.actiontech.dble.backend.mysql.store.result.ResultExternal;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.util.RBTreeList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * groupby is some part like distinct,but it should group by some value when add
 * a row
 */
public class GroupByLocalResult extends LocalResult {

    private RowDataComparator groupCmp;
    /**
     * the packets contains sums
     */
    private List<FieldPacket> fieldPackets;
    private List<ItemSum> sumFunctions;
    private boolean isAllPushDown;
    private final List<ItemSum> sums;

    /**
     * @param pool
     * @param fieldsCount   fieldsCount contains sums
     * @param groupCmp
     * @param fieldPackets  fieldPackets contains sums
     * @param sumFunctions
     * @param isAllPushDown
     */
    public GroupByLocalResult(BufferPool pool, int fieldsCount, RowDataComparator groupCmp,
                              List<FieldPacket> fieldPackets, List<ItemSum> sumFunctions, boolean isAllPushDown, String charset) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, groupCmp, fieldPackets, sumFunctions, isAllPushDown, charset);
    }

    public GroupByLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, RowDataComparator groupCmp,
                              List<FieldPacket> fieldPackets, List<ItemSum> sumFunctions, boolean isAllPushDown, String charset) {
        super(initialCapacity, fieldsCount, pool, charset);
        this.groupCmp = groupCmp;
        this.fieldPackets = fieldPackets;
        this.sumFunctions = sumFunctions;
        this.isAllPushDown = isAllPushDown;
        this.rows = new RBTreeList<>(initialCapacity, groupCmp);
        /* init item_sums */
        /*
      store the origin row fields,(already contains the item_sum fields in
      rowpackets we should calculate the item_sums again when next() is
      called!)
     */
        List<Field> fields = HandlerTool.createFields(fieldPackets);
        this.sums = new ArrayList<>();
        for (ItemSum sumFunc : sumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, fields, 0, this.isAllPushDown,
                    HandlerType.GROUPBY));
            this.sums.add(sum);
        }
        prepareSumAggregators(this.sums, true);
    }

    /* should group sumfunctions when find a row in rows */
    @Override
    public void add(RowDataPacket row) {
        lock.lock();
        try {
            if (isClosed)
                return;
            int index = rows.indexOf(row);
            int increSize = 0;
            if (index >= 0)/* found */ {
                RowDataPacket oldRow = rows.get(index);
                int oldRowSizeBefore = getRowMemory(oldRow);
                onFoundRow(oldRow, row);
                int oldRowSizeAfter = getRowMemory(oldRow);
                increSize = oldRowSizeAfter - oldRowSizeBefore;
            } else {
                onFirstGroupRow(row);
                rows.add(row);
                rowCount++;
                increSize = getRowMemory(row);
            }
            currentMemory += increSize;
            boolean needFlush = false;
            if (bufferMC != null) {
                if (!bufferMC.addSize(increSize)) {
                    needFlush = true;
                }
            } else if (!needFlush && currentMemory > maxMemory) {
                needFlush = true;
            }
            if (needFlush) {
                if (external == null)
                    external = makeExternal();
                addRowsToDisk();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected ResultExternal makeExternal() {
        return new GroupResultDiskBuffer(pool, fieldsCount, groupCmp, fieldPackets, sumFunctions, isAllPushDown,
                charset);
    }

    @Override
    protected void doneOnlyMemory() {
        Collections.sort(rows, this.groupCmp);
    }

    @Override
    protected void beforeFlushRows() {
        // rbtree.toarray() is sorted,so do not need to sort again
    }

    protected void onFoundRow(RowDataPacket oldRow, RowDataPacket row) {
        // we need to calculate group by
        initSumFunctions(this.sums, oldRow);
        updateSumFunc(this.sums, row);
        for (int i = 0; i < this.sums.size(); i++) {
            ItemSum sum = this.sums.get(i);
            Object b = sum.getTransAggObj();
            int transSize = sum.getTransSize();
            ((DGRowPacket) oldRow).setSumTran(i, b, transSize);
        }
    }

    protected void onFirstGroupRow(RowDataPacket row) {
        // we need to calculate group by
        initSumFunctions(this.sums, row);
        for (int i = 0; i < this.sums.size(); i++) {
            ItemSum sum = this.sums.get(i);
            Object b = sum.getTransAggObj();
            int transSize = sum.getTransSize();
            ((DGRowPacket) row).setSumTran(i, b, transSize);
        }
    }

    /**
     * see Sql_executor.cc
     *
     * @return
     */
    protected void prepareSumAggregators(List<ItemSum> funcs, boolean needDistinct) {
        for (ItemSum func : funcs) {
            func.setAggregator(needDistinct && func.hasWithDistinct() ?
                            AggregatorType.DISTINCT_AGGREGATOR : AggregatorType.SIMPLE_AGGREGATOR,
                    null);
        }
    }

    protected void initSumFunctions(List<ItemSum> funcs, RowDataPacket row) {
        for (int i = 0; i < funcs.size(); i++) {
            ItemSum sum = funcs.get(i);
            Object transObj = ((DGRowPacket) row).getSumTran(i);
            sum.resetAndAdd(row, transObj);
        }
    }

    protected void updateSumFunc(List<ItemSum> funcs, RowDataPacket row) {
        for (int index = 0; index < funcs.size(); index++) {
            ItemSum sum = funcs.get(index);
            Object transObj = ((DGRowPacket) row).getSumTran(index);
            sum.aggregatorAdd(row, transObj);
        }
    }

}
