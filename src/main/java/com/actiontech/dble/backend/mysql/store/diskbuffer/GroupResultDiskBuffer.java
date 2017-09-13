/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.diskbuffer;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.FileStore;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * the disk buffer which need to group by all the tapes value of it
 *
 * @author ActionTech
 */
public class GroupResultDiskBuffer extends DistinctResultDiskBuffer {

    private final List<ItemSum> sums;

    /**
     * @param pool
     * @param columnCount
     * @param cmp          group by cmptor
     * @param packets      packets which already contain sum_function's fieldpacket,
     *                     sum_packets are put in the front
     * @param sumFunctions
     */
    public GroupResultDiskBuffer(BufferPool pool, int fieldsCount, RowDataComparator cmp, List<FieldPacket> packets,
                                 List<ItemSum> sumFunctions, boolean isAllPushDown, String charset) {
        super(pool, fieldsCount, cmp);
        /*
      store the origin row fields,(already contains the item_sum fields in
      rowpackets we should calculate the item_sums again when next() is
      called!)
     */
        List<Field> fields = HandlerTool.createFields(packets);
        this.sums = new ArrayList<>();
        for (ItemSum sumFunc : sumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, fields, 0, isAllPushDown,
                    HandlerType.GROUPBY));
            this.sums.add(sum);
        }
        Logger logger = Logger.getLogger(GroupResultDiskBuffer.class);
        logger.info("prepare_sum_aggregators");
        prepareSumAggregators(this.sums, true);
    }

    @Override
    protected ResultDiskTape makeResultDiskTape() {
        return new GroupResultDiskTape(pool, file, columnCount, sums.size());
    }

    @Override
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

    /**
     * origin resultdisktape+ sum result
     *
     * @author ActionTech
     * @CreateTime 2015/5/20
     */
    static class GroupResultDiskTape extends ResultDiskTape {
        private final int orgFieldCount;
        private final int sumSize;

        GroupResultDiskTape(BufferPool pool, FileStore file, int fieldCount, int sumSize) {
            super(pool, file, sumSize + fieldCount);
            this.orgFieldCount = fieldCount;
            this.sumSize = sumSize;
        }

        @Override
        public RowDataPacket nextRow() {
            RowDataPacket rp = super.nextRow();
            if (rp == null)
                return null;
            else {
                DGRowPacket newRow = new DGRowPacket(orgFieldCount, sumSize);
                for (int index = 0; index < sumSize; index++) {
                    byte[] b = rp.getValue(index);
                    if (b != null) {
                        Object obj = SerializationUtils.deserialize(b);
                        newRow.setSumTran(index, obj, b.length);
                    }
                }
                for (int index = sumSize; index < this.fieldCount; index++) {
                    newRow.add(rp.getValue(index));
                }
                return newRow;
            }
        }
    }

}
