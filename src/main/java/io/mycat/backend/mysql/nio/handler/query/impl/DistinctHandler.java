package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.DistinctLocalResult;
import io.mycat.backend.mysql.store.LocalResult;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class DistinctHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(DistinctHandler.class);

    private List<Field> sourceFields;
    private LocalResult localResult;
    private RowDataComparator cmptor;
    private List<Order> fixedOrders;
    private BufferPool pool;
    /* if distincts is null, distinct the total row */
    private List<Item> distincts;

    public DistinctHandler(long id, NonBlockingSession session, List<Item> columns) {
        this(id, session, columns, null);
    }

    public DistinctHandler(long id, NonBlockingSession session, List<Item> columns, List<Order> fixedOrders) {
        super(id, session);
        this.distincts = columns;
        this.fixedOrders = fixedOrders;
    }

    @Override
    public HandlerType type() {
        return HandlerType.DISTINCT;
    }

    /**
     * 所有的上一级表传递过来的信息全部视作Field类型
     */
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = MycatServer.getInstance().getBufferPool();
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        if (this.distincts == null) {
            // 比如show tables这种语句
            this.distincts = new ArrayList<Item>();
            for (FieldPacket fp : this.fieldPackets) {
                Item sel = HandlerTool.createItemField(fp);
                this.distincts.add(sel);
            }
        }
        List<Order> orders = this.fixedOrders;
        if (orders == null)
            orders = HandlerTool.makeOrder(this.distincts);
        cmptor = new RowDataComparator(this.fieldPackets, orders, this.isAllPushDown(), type(), conn.getCharset());
        localResult = new DistinctLocalResult(pool, this.sourceFields.size(), cmptor, conn.getCharset())
                .setMemSizeController(session.getOtherBufferMC());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
    }

    /**
     * 收到行数据包的响应处理
     */
    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        localResult.add(rowPacket);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        sendDistinctRowPacket(conn);
        nextHandler.rowEofResponse(null, isLeft, conn);
    }

    private void sendDistinctRowPacket(BackendConnection conn) {
        localResult.done();
        RowDataPacket row = null;
        while ((row = localResult.next()) != null) {
            nextHandler.rowResponse(null, row, this.isLeft, conn);
        }
    }

    @Override
    public void onTerminate() {
        if (this.localResult != null)
            this.localResult.close();
    }

}
