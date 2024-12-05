/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;


import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.backend.mysql.store.DistinctSortedLocalResult;
import com.oceanbase.obsharding_d.backend.mysql.store.LocalResult;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.singleton.BufferPoolManager;
import org.jetbrains.annotations.NotNull;
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

    public DistinctHandler(long id, Session session, List<Item> columns) {
        this(id, session, columns, null);
    }

    public DistinctHandler(long id, Session session, List<Item> columns, List<Order> fixedOrders) {
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
    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = BufferPoolManager.getBufferPool();
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
        String charSet = !service.isFakeClosed() ? CharsetUtil.getJavaCharset(service.getCharset().getResults()) :
                CharsetUtil.getJavaCharset(session.getSource().getService().getCharset().getResults());
        localResult = new DistinctSortedLocalResult(pool, sourceFields.size(), comparator, charSet, generateBufferRecordBuilder()).
                setMemSizeController(session.getOtherBufferMC());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        if (terminate.get())
            return true;
        localResult.add(rowPacket);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        sendDistinctRowPacket(service);
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(null, this.isLeft, service);
    }

    private void sendDistinctRowPacket(AbstractService service) {
        localResult.done();
        RowDataPacket row = null;
        while ((row = localResult.next()) != null) {
            nextHandler.rowResponse(null, row, this.isLeft, service);
        }
    }

    @Override
    public void onTerminate() {
        if (this.localResult != null)
            this.localResult.close();
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.DISTINCT;
    }

}
