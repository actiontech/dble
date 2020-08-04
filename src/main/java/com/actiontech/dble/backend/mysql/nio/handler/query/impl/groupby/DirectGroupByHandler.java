/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.directgroupby.DGRowPacket;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.directgroupby.GroupByBucket;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.GroupByLocalResult;
import com.actiontech.dble.backend.mysql.store.LocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.sumfunc.Aggregator;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * groupBy is Behind OrderBy
 * use groupbylocalresult to calc groupby . try to merge before store to groupby LocalResult
 * these cases can't merge : 1.sum function contains distinct  2. groupconcat
 *
 * @author ActionTech
 */
public class DirectGroupByHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectGroupByHandler.class);

    private BlockingQueue<RowDataPacket> queue;

    private List<Order> groupBys;
    private List<ItemSum> referredSumFunctions;

    private BufferPool pool;
    private LocalResult groupLocalResult;
    private AtomicBoolean groupStart = new AtomicBoolean(false);

    private List<ItemSum> sums = new ArrayList<>();

    private AtomicBoolean hasFirstRow = new AtomicBoolean(false);

    private List<FieldPacket> localResultFps;

    private BlockingQueue<RowDataPacket> outQueue;
    int bucketSize = 10;
    private List<GroupByBucket> buckets;

    /**
     * @param groupBys
     * @param referredSumFunctions
     */
    public DirectGroupByHandler(long id, Session session, List<Order> groupBys,
                                List<ItemSum> referredSumFunctions) {
        super(id, session);
        this.groupBys = groupBys;
        this.referredSumFunctions = referredSumFunctions;
        int queueSize = SystemConfig.getInstance().getMergeQueueSize();
        this.queue = new LinkedBlockingQueue<>(queueSize);
        this.outQueue = new LinkedBlockingQueue<>(queueSize);
        this.buckets = new ArrayList<>();
    }

    @Override
    public HandlerType type() {
        return HandlerType.GROUPBY;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = BufferPoolManager.getBufferPool();

        this.fieldPackets = fieldPackets;
        List<Field> sourceFields = HandlerTool.createFields(this.fieldPackets);
        for (ItemSum sumFunc : referredSumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, sourceFields, 0, this.isAllPushDown(),
                    this.type()));
            sums.add(sum);
        }
        prepareSumAggregators(sums, true);
        setupSumFunctions(sums);
        /* group fieldpackets are front of the origin */
        sendGroupFieldPackets((MySQLResponseService) service);
        // row in localresult is DGRowPacket which is added aggregate functions result from origin rowdatapacket
        localResultFps = this.fieldPackets;
        List<ItemSum> localResultReferredSums = referredSumFunctions;
        RowDataComparator comparator = new RowDataComparator(this.localResultFps, this.groupBys, this.isAllPushDown(), this.type()
        );
        String charSet = service != null ? CharsetUtil.getJavaCharset(service.getConnection().getCharsetName().getResults()) : CharsetUtil.getJavaCharset(session.getSource().getCharsetName().getResults());
        groupLocalResult = new GroupByLocalResult(pool, localResultFps.size(), comparator, localResultFps,
                localResultReferredSums, this.isAllPushDown(), charSet).
                setMemSizeController(session.getOtherBufferMC());
        for (int i = 0; i < bucketSize; i++) {
            RowDataComparator tmpComparator = new RowDataComparator(this.localResultFps, this.groupBys,
                    this.isAllPushDown(), this.type());
            GroupByBucket bucket = new GroupByBucket(queue, outQueue, pool, localResultFps.size(), tmpComparator,
                    localResultFps, localResultReferredSums, this.isAllPushDown(), charSet);
            bucket.setMemSizeController(session.getOtherBufferMC());
            buckets.add(bucket);
            bucket.start();
        }
        if (this.groupStart.compareAndSet(false, true)) {
            startOwnThread(service);
        }
    }

    /**
     * aggregate functions result and origin rowdatapacket
     */
    private List<FieldPacket> sendGroupFieldPackets(MySQLResponseService service) {
        List<FieldPacket> newFps = new ArrayList<>();
        for (ItemSum sum1 : sums) {
            Item sum = sum1;
            FieldPacket tmp = new FieldPacket();
            sum.makeField(tmp);
            newFps.add(tmp);
        }
        newFps.addAll(this.fieldPackets);
        nextHandler.fieldEofResponse(null, null, newFps, null, this.isLeft, service);
        return newFps;
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        MySQLResponseService sqlResponseService = (MySQLResponseService) objects[0];
        recordElapsedTime("local group by thread is start:");
        try {
            int eofCount = 0;
            for (; ; ) {
                RowDataPacket row = outQueue.take();
                if (row.getFieldCount() == 0) {
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
                    sendNoRowGroupRowPacket(sqlResponseService);
            } else {
                sendGroupRowPacket(sqlResponseService);
            }
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(null, this.isLeft, sqlResponseService);
        } catch (Exception e) {
            String msg = "group by thread is error," + e.getLocalizedMessage();
            LOGGER.info(msg, e);
            session.onQueryError(msg.getBytes());
        }
    }

    private void recordElapsedTime(String prefix) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(prefix + TimeUtil.currentTimeMillis());
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        LOGGER.debug("rowResponse");
        if (terminate.get())
            return true;
        hasFirstRow.compareAndSet(false, true);
        try {
            DGRowPacket row = new DGRowPacket(rowPacket, this.referredSumFunctions.size());
            queue.put(row);
        } catch (InterruptedException e) {
            //ignore error
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
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

    private void sendGroupRowPacket(MySQLResponseService service) {
        groupLocalResult.done();
        RowDataPacket row = null;
        List<Field> localFields = HandlerTool.createFields(localResultFps);
        List<ItemSum> sendSums = new ArrayList<>();
        for (ItemSum selSum : referredSumFunctions) {
            ItemSum sum = (ItemSum) HandlerTool.createItem(selSum, localFields, 0, false, HandlerType.GROUPBY);
            sendSums.add(sum);
        }
        prepareSumAggregators(sendSums, true);
        while ((row = groupLocalResult.next()) != null) {
            if (sendGroupRowPacket(service, row, sendSums))
                break;
        }
    }

    private boolean sendGroupRowPacket(MySQLResponseService service, RowDataPacket row, List<ItemSum> sendSums) {
        initSumFunctions(sendSums, row);
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + sendSums.size());
        /**
         * add sums generated by middle-ware.
         * eg: a table node like select count(*) from t
         * the query will be push down,the final rowpacket is
         * count(*){col1,generated by group by handler},count(*){col2,response from mysql node}
         */
        for (ItemSum sendSum : sendSums) {
            byte[] tmp = sendSum.getRowPacketByte();
            newRp.add(tmp);
        }
        for (int i = 0; i < row.getFieldCount(); i++) {
            newRp.add(row.getValue(i));
        }
        return nextHandler.rowResponse(null, newRp, this.isLeft, service);
    }

    /**
     * send data to next even no data here.eg:select count(*) from t2,if t2 is empty,send 0
     */
    private void sendNoRowGroupRowPacket(MySQLResponseService service) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        for (ItemSum sum : this.sums) {
            sum.noRowsInResult();
            byte[] tmp = sum.getRowPacketByte();
            newRp.add(tmp);
        }
        for (int i = 0; i < this.fieldPackets.size(); i++) {
            newRp.add(null);
        }
        nextHandler.rowResponse(null, newRp, this.isLeft, service);
    }

    /**
     * see Sql_executor.cc
     *
     * @return
     */
    protected void prepareSumAggregators(List<ItemSum> functions, boolean needDistinct) {
        LOGGER.info("prepare_sum_aggregators");
        for (ItemSum func : functions) {
            func.setAggregator(needDistinct && func.hasWithDistinct() ?
                            Aggregator.AggregatorType.DISTINCT_AGGREGATOR : Aggregator.AggregatorType.SIMPLE_AGGREGATOR,
                    null);
        }
    }

    /**
     * Call ::setup for all sum functions.
     *
     * @param functions sum function list
     * @retval FALSE ok
     * @retval TRUE error
     */

    protected boolean setupSumFunctions(List<ItemSum> functions) {
        LOGGER.info("setup_sum_funcs");
        for (ItemSum func : functions) {
            if (func.aggregatorSetup())
                return true;
        }
        return false;
    }

    protected void initSumFunctions(List<ItemSum> functions, RowDataPacket row) {
        for (int index = 0; index < functions.size(); index++) {
            ItemSum sum = functions.get(index);
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
