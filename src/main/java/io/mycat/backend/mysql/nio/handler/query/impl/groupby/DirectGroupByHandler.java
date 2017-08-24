package io.mycat.backend.mysql.nio.handler.query.impl.groupby;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import io.mycat.backend.mysql.nio.handler.query.impl.groupby.directgroupby.GroupByBucket;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.GroupByLocalResult;
import io.mycat.backend.mysql.store.LocalResult;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.sumfunc.Aggregator;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.TimeUtil;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * groupBy的前提是已经经过了OrderBy
 * 通过groupbylocalresult直接进行groupby计算，在localresult计算中，优先进行内存内部的groupby计算，然后再在
 * 内存中再次进行group by计算 这种计算不支持的情况如下： 1.sum函数存在distinct约束 2.sum函数存在groupconcat类的函数
 *
 * @author ActionTech
 */
public class DirectGroupByHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(DirectGroupByHandler.class);

    private BlockingQueue<RowDataPacket> queue;

    /* 接收到的参数 */
    private List<Order> groupBys;
    private List<ItemSum> referedSumFunctions;

    private BufferPool pool;
    private LocalResult groupLocalResult;
    private AtomicBoolean groupStart = new AtomicBoolean(false);

    private List<ItemSum> sums = new ArrayList<ItemSum>();

    private AtomicBoolean hasFirstRow = new AtomicBoolean(false);

    /* 下发到localresult中的fieldPackets */
    private List<FieldPacket> localResultFps;

    private BlockingQueue<RowDataPacket> outQueue;
    int bucketSize = 10;
    private List<GroupByBucket> buckets;

    /**
     * @param groupBys
     * @param refers   涉及到的所有的sumfunction集合
     */
    public DirectGroupByHandler(long id, NonBlockingSession session, List<Order> groupBys,
                                List<ItemSum> referedSumFunctions) {
        super(id, session);
        this.groupBys = groupBys;
        this.referedSumFunctions = referedSumFunctions;
        int queueSize = MycatServer.getInstance().getConfig().getSystem().getMergeQueueSize();
        this.queue = new LinkedBlockingQueue<RowDataPacket>(queueSize);
        this.outQueue = new LinkedBlockingQueue<RowDataPacket>(queueSize);
        this.buckets = new ArrayList<GroupByBucket>();
    }

    @Override
    public HandlerType type() {
        return HandlerType.GROUPBY;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = MycatServer.getInstance().getBufferPool();

        this.fieldPackets = fieldPackets;
        List<Field> sourceFields = HandlerTool.createFields(this.fieldPackets);
        for (ItemSum sumFunc : referedSumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, sourceFields, 0, this.isAllPushDown(),
                    this.type(), conn.getCharset()));
            sums.add(sum);
        }
        prepareSumAggregators(sums, true);
        setupSumFuncs(sums);
        /* group fieldpackets are front of the origin */
        sendGroupFieldPackets((MySQLConnection) conn);
        // localresult中的row为DGRowPacket，比原始的rowdatapacket增加了聚合结果对象
        localResultFps = this.fieldPackets;
        List<ItemSum> localResultReferedSums = referedSumFunctions;
        RowDataComparator cmptor = new RowDataComparator(this.localResultFps, this.groupBys, this.isAllPushDown(), this.type(),
                conn.getCharset());
        groupLocalResult = new GroupByLocalResult(pool, localResultFps.size(), cmptor, localResultFps,
                localResultReferedSums, this.isAllPushDown(), conn.getCharset()).
                setMemSizeController(session.getOtherBufferMC());
        for (int i = 0; i < bucketSize; i++) {
            RowDataComparator tmpcmptor = new RowDataComparator(this.localResultFps, this.groupBys,
                    this.isAllPushDown(), this.type(), conn.getCharset());
            GroupByBucket bucket = new GroupByBucket(queue, outQueue, pool, localResultFps.size(), tmpcmptor,
                    localResultFps, localResultReferedSums, this.isAllPushDown(), conn.getCharset());
            bucket.setMemSizeController(session.getOtherBufferMC());
            buckets.add(bucket);
            bucket.start();
        }
        if (this.groupStart.compareAndSet(false, true)) {
            startOwnThread(conn);
        }
    }

    /**
     * 生成新的fieldPackets，包括生成的聚合函数以及原始的fieldpackets
     */
    private List<FieldPacket> sendGroupFieldPackets(MySQLConnection conn) {
        List<FieldPacket> newFps = new ArrayList<FieldPacket>();
        for (ItemSum sum1 : sums) {
            Item sum = sum1;
            FieldPacket tmpfp = new FieldPacket();
            sum.makeField(tmpfp);
            newFps.add(tmpfp);
        }
        newFps.addAll(this.fieldPackets);
        nextHandler.fieldEofResponse(null, null, newFps, null, this.isLeft, conn);
        return newFps;
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        MySQLConnection conn = (MySQLConnection) objects[0];
        recordElapsedTime("local group by thread is start:");
        try {
            int eofCount = 0;
            for (; ; ) {
                RowDataPacket row = outQueue.take();
                if (row.fieldCount == 0) {
                    eofCount++;
                    if (eofCount == bucketSize)
                        break;
                    else
                        continue;
                }
                groupLocalResult.add(row);
            }
            recordElapsedTime("local group by thread is end:");
            groupLocalResult.done();
            recordElapsedTime("local group by thread is done for read:");
            if (!hasFirstRow.get()) {
                if (HandlerTool.needSendNoRow(this.groupBys))
                    sendNoRowGroupRowPacket(conn);
            } else {
                sendGroupRowPacket(conn);
            }
            nextHandler.rowEofResponse(null, this.isLeft, conn);
        } catch (Exception e) {
            String msg = "group by thread is error," + e.getLocalizedMessage();
            LOGGER.warn(msg, e);
            session.onQueryError(msg.getBytes());
        }
    }

    private void recordElapsedTime(String prefix) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(prefix + TimeUtil.currentTimeMillis());
        }
    }

    @Override
    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("rowResponse");
        if (terminate.get())
            return true;
        hasFirstRow.compareAndSet(false, true);
        try {
            DGRowPacket row = new DGRowPacket(rowPacket, this.referedSumFunctions.size());
            queue.put(row);
        } catch (InterruptedException e) {
            //ignore error
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        try {
            // @bug1042
            for (int i = 0; i < bucketSize; i++)
                queue.put(new RowDataPacket(0));
        } catch (InterruptedException e) {
            //ignore error
        }
    }

    /**
     * 将一组group好的数据发送出去
     */
    private void sendGroupRowPacket(MySQLConnection conn) {
        groupLocalResult.done();
        RowDataPacket row = null;
        List<Field> localFields = HandlerTool.createFields(localResultFps);
        List<ItemSum> sendSums = new ArrayList<ItemSum>();
        for (ItemSum selSum : referedSumFunctions) {
            ItemSum sum = (ItemSum) HandlerTool.createItem(selSum, localFields, 0, false, HandlerType.GROUPBY,
                    conn.getCharset());
            sendSums.add(sum);
        }
        prepareSumAggregators(sendSums, true);
        while ((row = groupLocalResult.next()) != null) /* group函数已经在row中被计算过了 */ {
            if (sendGroupRowPacket(conn, row, sendSums))
                break;
        }
    }

    /**
     * 将一组group好的数据发送出去
     */
    private boolean sendGroupRowPacket(MySQLConnection conn, RowDataPacket row, List<ItemSum> sendSums) {
        initSumFunctions(sendSums, row);
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + sendSums.size());
        /**
         * 将自己生成的聚合函数的值放在前面，这样在tablenode时，如果用户语句如select count(*) from t
         * 由于整个语句下发，所以最后生成的rowpacket顺序为
         * count(*){groupbyhandler生成的},count(*){下发到各个节点的，不是真实的值}
         */
        for (ItemSum sendSum : sendSums) {
            byte[] tmpb = sendSum.getRowPacketByte();
            newRp.add(tmpb);
        }
        for (int i = 0; i < row.fieldCount; i++) {
            newRp.add(row.getValue(i));
        }
        return nextHandler.rowResponse(null, newRp, this.isLeft, conn);
    }

    /**
     * 没有数据时，也要发送结果 比如select count(*) from t2 ，如果t2是一张空表的话，那么显示为0
     */
    private void sendNoRowGroupRowPacket(MySQLConnection conn) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        for (ItemSum sum : this.sums) {
            sum.noRowsInResult();
            byte[] tmpb = sum.getRowPacketByte();
            newRp.add(tmpb);
        }
        for (int i = 0; i < this.fieldPackets.size(); i++) {
            newRp.add(null);
        }
        nextHandler.rowResponse(null, newRp, this.isLeft, conn);
    }

    /**
     * see Sql_executor.cc
     *
     * @return
     */
    protected void prepareSumAggregators(List<ItemSum> funcs, boolean needDistinct) {
        LOGGER.info("prepare_sum_aggregators");
        for (ItemSum func : funcs) {
            func.setAggregator(needDistinct && func.hasWithDistinct() ?
                            Aggregator.AggregatorType.DISTINCT_AGGREGATOR : Aggregator.AggregatorType.SIMPLE_AGGREGATOR,
                    null);
        }
    }

    /**
     * Call ::setup for all sum functions.
     *
     * @param thd      thread handler
     * @param func_ptr sum function list
     * @retval FALSE ok
     * @retval TRUE error
     */

    protected boolean setupSumFuncs(List<ItemSum> funcs) {
        LOGGER.info("setup_sum_funcs");
        for (ItemSum func : funcs) {
            if (func.aggregatorSetup())
                return true;
        }
        return false;
    }

    protected void initSumFunctions(List<ItemSum> funcs, RowDataPacket row) {
        for (int index = 0; index < funcs.size(); index++) {
            ItemSum sum = funcs.get(index);
            Object transObj = ((DGRowPacket) row).getSumTran(index);
            sum.resetAndAdd(row, transObj);
        }
    }

    @Override
    protected void terminateThread() throws Exception {
        this.queue.clear();
        for (int i = 0; i < bucketSize; i++)
            queue.put(new RowDataPacket(0));
    }

    @Override
    protected void recycleResources() {
        this.queue.clear();
        if (this.groupLocalResult != null)
            this.groupLocalResult.close();
        for (LocalResult bucket : buckets) {
            bucket.close();
        }
    }

}
