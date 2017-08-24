package io.mycat.backend.mysql.nio.handler.query.impl.groupby;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.DistinctLocalResult;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.external.ResultStore;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.sumfunc.Aggregator.AggregatorType;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 1.处理已经依据groupby的列进行过排序的groupby 2.处理需要用到Aggregator_distinct的group by
 */
public class OrderedGroupByHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(OrderedGroupByHandler.class);
    /* 接收到的参数 */
    private List<Order> groupBys;
    private List<ItemSum> referedSumFunctions;

    private RowDataComparator cmptor;

    private List<ItemSum> sums = new ArrayList<>();

    /* group组的原始rowpacket，目前保留第一条数据的值 */
    private RowDataPacket originRp = null;

    private boolean hasFirstRow = false;

    private BufferPool pool;

    private String charset = "UTF-8";

    /**
     * merge以及sendmaker现在都是多线程
     **/
    private ReentrantLock lock = new ReentrantLock();

    /* 例如count(distinct id)中用到的distinct store */
    private List<ResultStore> distinctStores;

    /**
     * @param groupBys
     * @param refers   涉及到的所有的sumfunction集合
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
        this.charset = conn.getCharset();
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
        cmptor = new RowDataComparator(this.fieldPackets, this.groupBys, this.isAllPushDown(), this.type(),
                conn.getCharset());
        prepareSumAggregators(sums, this.referedSumFunctions, this.fieldPackets, this.isAllPushDown(), true, (MySQLConnection) conn);
        setupSumFuncs(sums);
        sendGroupFieldPackets(conn);
    }

    /**
     * 生成新的fieldPackets，包括生成的聚合函数以及原始的fieldpackets
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
                    // 需要将这一组数据发送出去
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

    /**
     * 将一组group好的数据发送出去
     */
    private void sendGroupRowPacket(MySQLConnection conn) {
        RowDataPacket newRp = new RowDataPacket(this.fieldPackets.size() + this.sums.size());
        /**
         * 将自己生成的聚合函数的值放在前面，这样在tablenode时，如果用户语句如select count(*) from t
         * 由于整个语句下发，所以最后生成的rowpacket顺序为
         * count(*){groupbyhandler生成的},count(*){下发到各个节点的，不是真实的值}
         */
        for (ItemSum sum : this.sums) {
            byte[] tmpb = sum.getRowPacketByte();
            newRp.add(tmpb);
        }
        for (int i = 0; i < originRp.fieldCount; i++) {
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
     * 没有数据时，也要发送结果 比如select count(*) from t2 ，如果t2是一张空表的话，那么显示为0
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
                RowDataComparator distinctCmp = new RowDataComparator(packets, orders, isAllPushDown, this.type(),
                        conn.getCharset());
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
