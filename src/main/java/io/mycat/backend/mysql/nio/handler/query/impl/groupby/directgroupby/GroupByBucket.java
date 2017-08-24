package io.mycat.backend.mysql.nio.handler.query.impl.groupby.directgroupby;

import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.GroupByLocalResult;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 多线程的Group By桶，并发生成Group By的中间结果，最终再进行Group By，从而生成Group By的总结果
 */
public class GroupByBucket extends GroupByLocalResult {
    // 进行groupby的输入来源
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
     * 开启一个新的线程进行Group by工作
     */
    public void start() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        RowDataPacket rp = inData.take();
                        if (rp.fieldCount == 0)
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
