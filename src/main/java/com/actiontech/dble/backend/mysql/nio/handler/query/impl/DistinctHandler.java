/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.DistinctSortedLocalResult;
import com.actiontech.dble.backend.mysql.store.LocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DistinctHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistinctHandler.class);

    private LocalResult localResult;
    private List<Order> fixedOrders;
    private BufferPool pool;
    /* if distinct is null, distinct the total row */
    private List<Item> distinctCols;

    public DistinctHandler(long id, NonBlockingSession session, List<Item> columns) {
        this(id, session, columns, null);
    }

    public DistinctHandler(long id, NonBlockingSession session, List<Item> columns, List<Order> fixedOrders) {
        super(id, session);
        this.distinctCols = columns;
        this.fixedOrders = fixedOrders;
    }

    @Override
    public HandlerType type() {
        return HandlerType.DISTINCT;
    }

    /**
     * treat all the data from parent as Field Type
     */
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, BackendConnection conn) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = DbleServer.getInstance().getBufferPool();
        this.fieldPackets = fieldPackets;
        List<Field> sourceFields = HandlerTool.createFields(this.fieldPackets);
        if (this.distinctCols == null) {
            // eg:show tables
            this.distinctCols = new ArrayList<>();
            for (FieldPacket fp : this.fieldPackets) {
                Item sel = HandlerTool.createItemField(fp);
                this.distinctCols.add(sel);
            }
        }
        List<Order> orders = this.fixedOrders;
        if (orders == null)
            orders = HandlerTool.makeOrder(this.distinctCols);
        RowDataComparator comparator = new RowDataComparator(this.fieldPackets, orders, this.isAllPushDown(), type());
        String charSet = conn != null ? CharsetUtil.getJavaCharset(conn.getCharset().getResults()) : CharsetUtil.getJavaCharset(session.getSource().getCharset().getResults());
        localResult = new DistinctSortedLocalResult(pool, sourceFields.size(), comparator, charSet).
                setMemSizeController(session.getOtherBufferMC());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
    }

    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
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
        session.setHandlerEnd(this);
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
