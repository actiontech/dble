/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.groupby.directgroupby;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.backend.mysql.store.GroupByLocalResult;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemSum;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * GroupByBucket,generate Group By tmp result in every bucket in parallel ,and merge the buckets finally
 */
public class GroupByBucket extends GroupByLocalResult {
    private BlockingQueue<RowDataPacket> inData;
    private BlockingQueue<RowDataPacket> outData;

    public GroupByBucket(BlockingQueue<RowDataPacket> sourceData, BlockingQueue<RowDataPacket> outData,
                         BufferPool pool, int fieldsCount, RowDataComparator groupCmp,
                         List<FieldPacket> fieldPackets, List<ItemSum> sumFunctions,
                         boolean isAllPushDown, String charset, BufferPoolRecord.Builder bufferRecordBuilder) {
        super(pool, fieldsCount, groupCmp, fieldPackets, sumFunctions,
                isAllPushDown, charset, bufferRecordBuilder);
        this.inData = sourceData;
        this.outData = outData;
    }

    /**
     * new Group by thread
     */
    public void start() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        RowDataPacket rp = inData.take();
                        if (rp.getFieldCount() == 0)
                            break;
                        add(rp);
                    }
                    done();
                    RowDataPacket groupedRow = null;
                    while ((groupedRow = next()) != null)
                        outData.put(groupedRow);
                    outData.put(OwnThreadDMLHandler.TERMINATED_ROW);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    @Override
    public void close() {
        inData.add(OwnThreadDMLHandler.TERMINATED_ROW);
        super.close();
    }

}
