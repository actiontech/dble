/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.DistinctLocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.external.ResultStore;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 1.executed the ordered result of group by 2. group by of Aggregator_distinct
 */
public class OrderedGroupByHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(OrderedGroupByHandler.class);
    private List<Order> groupBys;
    private List<ItemSum> referedSumFunctions;

    private RowDataComparator cmptor;

    private List<ItemSum> sums = new ArrayList<>();

    /* origin group row packet,save the first row data */
    private RowDataPacket originRp = null;

    private boolean hasFirstRow = false;

    private BufferPool pool;

    private String charset = "UTF-8";

    /**
     * merge and send maker are multi-thread
     **/
    private ReentrantLock lock = new ReentrantLock();

    /* eg: distinct store used for count(distinct id)*/
    private List<ResultStore> distinctStores;

    /**
     * @param groupBys
     * @param referedSumFunctions
     */
    public OrderedGroupByHandler(long id, NonBlockingSession session, List<Order> groupBys, List<ItemSum> referedSumFunctions) {
        super(id, session);
        this.groupBys = groupBys;
        this.referedSumFunctions = referedSumFunctions;
        this.distinctStores = new ArrayList<>();
    }

    @Override
    public HandlerType type() {
        return HandlerType.GROUPBY;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        this.charset = CharsetUtil.getJavaCharset(conn.getCharset().getResults());
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = DbleServer.getInstance().getBufferPool();
        this.fieldPackets = fieldPackets;
        List<Field> sourceFields = HandlerTool.createFields(this.fieldPackets);
        for (ItemSum sumFunc : referedSumFunctions) {
            ItemSum sum = (ItemSum) (HandlerTool.createItem(sumFunc, sourceFields, 0, this.isAllPushDown(),
                    this.type()));
            sums.add(sum);
        }
        cmptor = new RowDataComparator(this.fieldPackets, this.groupBys, this.isAllPushDown(), this.type());
        prepareSumAggregators(sums, this.referedSumFunctions, this.fieldPackets, this.isAllPushDown(), true, (MySQLConnection) conn);
        setupSumFuncs(sums);
        sendGroupFieldPackets(conn);
    }

    /**
     * new fieldPackets: generated function result + origin fieldpackets
     */
    private void sendGroupFieldPackets(BackendConnection conn) {
        List<FieldPacket> newFps = new ArrayList<>();
        for (ItemSum sum1 : sums) {
            Item sum = sum1;
            FieldPacket tmpfp = new FieldPacket();
            sum.makeField(tmpfp);
            newFps.add(tmpfp);
        }
        newFps.addAll(this.fieldPackets);
        nextHandler.fieldEofResponse(null, null, newFps, null, this.isLeft, conn);
    }

    @Override
    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("rowresponse");
        if (terminate.get())
            return true;
        lock.lock();
        try {
            if (!hasFirstRow) {
                hasFirstRow = true;
                originRp = rowPacket;
                initSumFunctions(sums, rowPacket);
            } else {
                boolean sameGroupRow = this.groupBys.size() == 0 || (cmptor.compare(originRp, rowPacket) == 0);
                if (!sameGroupRow) {
                    // send the completed result firstly
                    sendGroupRowPacket((MySQLConnection) conn);
                    originRp = rowPacket;
                    initSumFunctions(sums, rowPacket);
                } else {
                    updateSumFunc(sums, rowPacket);
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void sendGroupRowPacket(MySQLConnection conn) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        for (ItemSum sum : this.sums) {
            byte[] tmpb = sum.getRowPacketByte();
            newRp.add(tmpb);
        }
        for (int i = 0; i < originRp.getFieldCount(); i++) {
            newRp.add(originRp.getValue(i));
        }
        nextHandler.rowResponse(null, newRp, this.isLeft, conn);
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("row eof for orderby.");
        if (terminate.get())
            return;
        if (!hasFirstRow) {
            if (HandlerTool.needSendNoRow(this.groupBys))
                sendNoRowGroupRowPacket((MySQLConnection) conn);
        } else {
            sendGroupRowPacket((MySQLConnection) conn);
        }
        nextHandler.rowEofResponse(data, this.isLeft, conn);
    }

    /**
     * send data to next even no data here. eg: select count(*) from t2 ,if t2 empty,then send 0
     */
    private void sendNoRowGroupRowPacket(MySQLConnection conn) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        // @bug 1050
        // sumfuncs are front
        for (ItemSum sum : this.sums) {
            sum.noRowsInResult();
            byte[] tmpb = sum.getRowPacketByte();
            newRp.add(tmpb);
        }
        for (int i = 0; i < this.fieldPackets.size(); i++) {
            newRp.add(null);
        }
        originRp = null;
        nextHandler.rowResponse(null, newRp, this.isLeft, conn);
    }

    /**
     * see Sql_executor.cc
     *
     * @return
     */
    protected void prepareSumAggregators(List<ItemSum> funcs, List<ItemSum> sumfuncs, List<FieldPacket> packets,
                                         boolean isAllPushDown, boolean needDistinct, MySQLConnection conn) {
        LOGGER.info("prepare_sum_aggregators");
        for (int i = 0; i < funcs.size(); i++) {
            ItemSum func = funcs.get(i);
            ResultStore store = null;
            if (func.hasWithDistinct()) {
                ItemSum selFunc = sumfuncs.get(i);
                List<Order> orders = HandlerTool.makeOrder(selFunc.arguments());
                RowDataComparator distinctCmp = new RowDataComparator(packets, orders, isAllPushDown, this.type());
                store = new DistinctLocalResult(pool, packets.size(), distinctCmp, this.charset).
                        setMemSizeController(session.getOtherBufferMC());
                distinctStores.add(store);
            }
            func.setAggregator(needDistinct && func.hasWithDistinct() ?
                            AggregatorType.DISTINCT_AGGREGATOR : AggregatorType.SIMPLE_AGGREGATOR,
                    store);
        }
    }

    /**
     * Call ::setup for all sum functions.
     *
     * @param funcs sum function list
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
        for (ItemSum func : funcs) {
            func.resetAndAdd(row, null);
        }
    }

    protected void updateSumFunc(List<ItemSum> funcs, RowDataPacket row) {
        for (ItemSum func : funcs) {
            func.aggregatorAdd(row, null);
        }
    }

    @Override
    public void onTerminate() {
        for (ResultStore store : distinctStores) {
            store.close();
        }
    }
}
