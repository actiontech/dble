/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.directgroupby;

import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.GroupByLocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;

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
                         boolean isAllPushDown, String charset) {
        super(pool, fieldsCount, groupCmp, fieldPackets, sumFunctions,
                isAllPushDown, charset);
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
                    outData.put(new RowDataPacket((0)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

}
