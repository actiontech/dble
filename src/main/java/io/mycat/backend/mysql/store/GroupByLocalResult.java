package io.mycat.backend.mysql.store;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import io.mycat.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.diskbuffer.GroupResultDiskBuffer;
import io.mycat.backend.mysql.store.result.ResultExternal;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.util.RBTreeList;

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
    /**
     * store the origin row fields,(already contains the item_sum fields in
     * rowpackets we should calculate the item_sums again when next() is
     * called!)
     */
    private final List<Field> fields;
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
        this.rows = new RBTreeList<RowDataPacket>(initialCapacity, groupCmp);
        /* init item_sums */
        this.fields = HandlerTool.createFields(fieldPackets);
        this.sums = new ArrayList<ItemSum>();
        for (ItemSum sumFunc : sumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, this.fields, 0, this.isAllPushDown,
                    HandlerType.GROUPBY, charset));
            this.sums.add(sum);
        }
        prepare_sum_aggregators(this.sums, true);
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
            if (index >= 0)// find
            {
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
                if (bufferMC.addSize(increSize) != true) {
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
        init_sum_functions(this.sums, oldRow);
        update_sum_func(this.sums, row);
        for (int i = 0; i < this.sums.size(); i++) {
            ItemSum sum = this.sums.get(i);
            Object b = sum.getTransAggObj();
            int transSize = sum.getTransSize();
            ((DGRowPacket) oldRow).setSumTran(i, b, transSize);
        }
    }

    protected void onFirstGroupRow(RowDataPacket row) {
        // we need to calculate group by
        init_sum_functions(this.sums, row);
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
    protected void prepare_sum_aggregators(List<ItemSum> funcs, boolean need_distinct) {
        for (ItemSum func : funcs) {
            func.setAggregator(need_distinct && func.has_with_distinct()
                            ? AggregatorType.DISTINCT_AGGREGATOR : AggregatorType.SIMPLE_AGGREGATOR,
                    null);
        }
    }

    protected void init_sum_functions(List<ItemSum> funcs, RowDataPacket row) {
        for (int i = 0; i < funcs.size(); i++) {
            ItemSum sum = funcs.get(i);
            Object transObj = ((DGRowPacket) row).getSumTran(i);
            sum.resetAndAdd(row, transObj);
        }
    }

    protected void update_sum_func(List<ItemSum> funcs, RowDataPacket row) {
        for (int index = 0; index < funcs.size(); index++) {
            ItemSum sum = funcs.get(index);
            Object transObj = ((DGRowPacket) row).getSumTran(index);
            sum.aggregatorAdd(row, transObj);
        }
    }

}
