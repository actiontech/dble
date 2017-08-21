package io.mycat.backend.mysql.store.diskbuffer;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import io.mycat.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.FileStore;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
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
    private final Logger logger = Logger.getLogger(GroupResultDiskBuffer.class);
    /**
     * store the origin row fields,(already contains the item_sum fields in
     * rowpackets we should calculate the item_sums again when next() is
     * called!)
     */
    private final List<Field> fields;

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
        this.fields = HandlerTool.createFields(packets);
        this.sums = new ArrayList<ItemSum>();
        for (ItemSum sumFunc : sumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, this.fields, 0, isAllPushDown,
                    HandlerType.GROUPBY, charset));
            this.sums.add(sum);
        }
        logger.info("prepare_sum_aggregators");
        prepare_sum_aggregators(this.sums, true);
    }

    @Override
    protected ResultDiskTape makeResultDiskTape() {
        return new GroupResultDiskTape(pool, file, columnCount, sums.size());
    }

    @Override
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

    /**
     * 比原生的resultdisktape要添加sum结果的值
     *
     * @author ActionTech
     * @CreateTime 2015年5月20日
     */
    static class GroupResultDiskTape extends ResultDiskTape {
        private final int orgFieldCount;
        private final int sumSize;

        public GroupResultDiskTape(BufferPool pool, FileStore file, int fieldCount, int sumSize) {
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
