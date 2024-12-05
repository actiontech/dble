/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.store.diskbuffer;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.backend.mysql.store.FileStore;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemSum;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * the disk buffer which need to group by all the tapes value of it
 *
 * @author oceanbase
 */
public class GroupResultDiskBuffer extends DistinctResultDiskBuffer {

    private final List<ItemSum> sums;

    /**
     * @param pool
     * @param fieldsCount
     * @param cmp          group by comparator
     * @param packets      packets which already contain sum_function's fieldpacket,
     *                     sum_packets are put in the front
     * @param sumFunctions
     */
    public GroupResultDiskBuffer(BufferPool pool, int fieldsCount, RowDataComparator cmp, List<FieldPacket> packets,
                                 List<ItemSum> sumFunctions, boolean isAllPushDown, String charset, BufferPoolRecord.Builder bufferRecordBuilder) {
        super(pool, fieldsCount, cmp, bufferRecordBuilder);
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
        Logger logger = LoggerFactory.getLogger(GroupResultDiskBuffer.class);
        logger.info("prepare_sum_aggregators");
        prepareSumAggregators(this.sums, true);
    }

    @Override
    protected ResultDiskTape makeResultDiskTape() {
        return new GroupResultDiskTape(pool, file, columnCount, sums.size(), bufferRecordBuilder);
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
    protected void prepareSumAggregators(List<ItemSum> functions, boolean needDistinct) {
        for (ItemSum func : functions) {
            func.setAggregator(needDistinct && func.hasWithDistinct() ?
                            AggregatorType.DISTINCT_AGGREGATOR : AggregatorType.SIMPLE_AGGREGATOR,
                    null);
        }
    }

    protected void initSumFunctions(List<ItemSum> functions, RowDataPacket row) {
        for (int i = 0; i < functions.size(); i++) {
            ItemSum sum = functions.get(i);
            Object transObj = ((DGRowPacket) row).getSumTran(i);
            sum.resetAndAdd(row, transObj);
        }
    }

    protected void updateSumFunc(List<ItemSum> functions, RowDataPacket row) {
        for (int index = 0; index < functions.size(); index++) {
            ItemSum sum = functions.get(index);
            Object transObj = ((DGRowPacket) row).getSumTran(index);
            sum.aggregatorAdd(row, transObj);
        }
    }

    /**
     * origin resultdisktape+ sum result
     *
     * @author oceanbase
     * @CreateTime 2015/5/20
     */
    static class GroupResultDiskTape extends ResultDiskTape {
        private final int orgFieldCount;
        private final int sumSize;

        GroupResultDiskTape(BufferPool pool, FileStore file, int fieldCount, int sumSize, BufferPoolRecord.Builder bufferRecordBuilder) {
            super(pool, file, sumSize + fieldCount, bufferRecordBuilder);
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
