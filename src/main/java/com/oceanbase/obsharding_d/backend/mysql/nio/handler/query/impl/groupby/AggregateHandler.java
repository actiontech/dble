/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.groupby;


import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.backend.mysql.store.DistinctLocalResult;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.external.ResultStore;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemSum;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.singleton.BufferPoolManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 1.executed the ordered result of group by 2. group by of Aggregator_distinct
 */
public class AggregateHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateHandler.class);
    private List<Order> groupBys;
    private List<ItemSum> referredSumFunctions;

    private RowDataComparator comparator;

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
     * @param referredSumFunctions
     */
    public AggregateHandler(long id, Session session, List<Order> groupBys, List<ItemSum> referredSumFunctions) {
        super(id, session);
        this.groupBys = groupBys;
        this.referredSumFunctions = referredSumFunctions;
        this.distinctStores = new ArrayList<>();
    }

    @Override
    public HandlerType type() {
        return HandlerType.GROUPBY;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerStart(this);
        this.charset = !service.isFakeClosed() ? CharsetUtil.getJavaCharset(service.getCharset().getResults()) : CharsetUtil.getJavaCharset(session.getSource().getService().getCharset().getResults());
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
        comparator = new RowDataComparator(this.fieldPackets, this.groupBys, this.isAllPushDown(), this.type());
        prepareSumAggregators(sums, this.referredSumFunctions, this.fieldPackets, this.isAllPushDown());
        setupSumFunctions(sums);
        sendGroupFieldPackets(service);
    }

    /**
     * new fieldPackets: generated function result + origin fieldpackets
     */
    private void sendGroupFieldPackets(AbstractService service) {
        List<FieldPacket> newFps = new ArrayList<>();
        for (ItemSum sum1 : sums) {
            Item sum = sum1;
            FieldPacket tmp = new FieldPacket();
            sum.makeField(tmp);
            newFps.add(tmp);
        }
        newFps.addAll(this.fieldPackets);
        nextHandler.fieldEofResponse(null, null, newFps, null, this.isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
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
                boolean sameGroupRow = this.groupBys.size() == 0 || (comparator.compare(originRp, rowPacket) == 0);
                if (!sameGroupRow) {
                    // send the completed result firstly
                    sendGroupRowPacket((MySQLResponseService) service);
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

    private void sendGroupRowPacket(MySQLResponseService service) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        for (ItemSum sum : this.sums) {
            byte[] tmp = sum.getRowPacketByte();
            newRp.add(tmp);
        }
        for (int i = 0; i < originRp.getFieldCount(); i++) {
            newRp.add(originRp.getValue(i));
        }
        nextHandler.rowResponse(null, newRp, this.isLeft, service);
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.debug("row eof for orderby.");
        if (terminate.get())
            return;
        if (!hasFirstRow) {
            if (HandlerTool.needSendNoRow(this.groupBys))
                sendNoRowGroupRowPacket((MySQLResponseService) service);
        } else {
            sendGroupRowPacket((MySQLResponseService) service);
        }
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(data, this.isLeft, service);
    }

    /**
     * send data to next even no data here. eg: select count(*) from t2 ,if t2 empty,then send 0
     */
    private void sendNoRowGroupRowPacket(MySQLResponseService service) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        // @bug 1050
        // sumfuncs are front
        for (ItemSum sum : this.sums) {
            sum.noRowsInResult();
            byte[] tmp = sum.getRowPacketByte();
            newRp.add(tmp);
        }
        for (int i = 0; i < this.fieldPackets.size(); i++) {
            newRp.add(null);
        }
        originRp = null;
        nextHandler.rowResponse(null, newRp, this.isLeft, service);
    }

    /**
     * see Sql_executor.cc
     *
     * @return
     */
    protected void prepareSumAggregators(List<ItemSum> functions, List<ItemSum> sumFunctions, List<FieldPacket> packets,
                                         boolean isAllPushDown) {
        LOGGER.debug("prepare_sum_aggregators");
        for (int i = 0; i < functions.size(); i++) {
            ItemSum func = functions.get(i);
            ResultStore store = null;
            if (func.hasWithDistinct()) {
                ItemSum selFunc = sumFunctions.get(i);
                List<Order> orders = HandlerTool.makeOrder(selFunc.arguments());
                RowDataComparator distinctCmp = new RowDataComparator(packets, orders, isAllPushDown, this.type());
                store = new DistinctLocalResult(pool, packets.size(), distinctCmp, this.charset, generateBufferRecordBuilder()).
                        setMemSizeController(session.getOtherBufferMC());
                distinctStores.add(store);
            }
            func.setAggregator(func.hasWithDistinct() ?
                            AggregatorType.DISTINCT_AGGREGATOR : AggregatorType.SIMPLE_AGGREGATOR,
                    store);
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
        LOGGER.debug("setup_sum_funcs");
        for (ItemSum func : functions) {
            if (func.aggregatorSetup())
                return true;
        }
        return false;
    }

    protected void initSumFunctions(List<ItemSum> functions, RowDataPacket row) {
        for (ItemSum func : functions) {
            func.resetAndAdd(row, null);
        }
    }

    protected void updateSumFunc(List<ItemSum> functions, RowDataPacket row) {
        for (ItemSum func : functions) {
            func.aggregatorAdd(row, null);
        }
    }

    @Override
    public void onTerminate() {
        for (ResultStore store : distinctStores) {
            store.close();
        }
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.AGGREGATE;
    }
}
