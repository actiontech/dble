/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.LocalResult;
import com.actiontech.dble.backend.mysql.store.SortedLocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class OrderByHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderByHandler.class);

    private List<Order> orders;
    private BlockingQueue<RowDataPacket> queue;
    /* tmp object for ordering,support Memory-mapped file or file */
    private LocalResult localResult;
    private BufferPool pool;

    public OrderByHandler(long id, Session session, List<Order> orders) {
        super(id, session);
        this.orders = orders;
        int queueSize = SystemConfig.getInstance().getOrderByQueueSize();
        this.queue = new LinkedBlockingDeque<>(queueSize);
    }

    @Override
    public HandlerType type() {
        return HandlerType.ORDERBY;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, final AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = BufferPoolManager.getBufferPool();

        this.fieldPackets = fieldPackets;
        RowDataComparator cmp = new RowDataComparator(this.fieldPackets, orders, isAllPushDown(), type());
        String charSet = service != null ? CharsetUtil.getJavaCharset(service.getConnection().getCharsetName().getResults()) :
                CharsetUtil.getJavaCharset(session.getSource().getService().getConnection().getCharsetName().getResults());
        localResult = new SortedLocalResult(pool, fieldPackets.size(), cmp, charSet).
                setMemSizeController(session.getOrderBufferMC());
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
        startOwnThread(service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        if (terminate.get())
            return true;
        try {
            queue.put(rowPacket);
        } catch (InterruptedException e) {
            return true;
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        try {
            queue.put(new RowDataPacket(0));
        } catch (InterruptedException e) {
            //ignore error
        }
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        MySQLResponseService service = (MySQLResponseService) objects[0];
        recordElapsedTime("order writeDirectly start :");
        try {
            while (true) {
                if (terminate.get()) {
                    return;
                }
                RowDataPacket row = null;
                try {
                    row = queue.take();
                    if (row.getFieldCount() == 0) {
                        break;
                    }
                    localResult.add(row);
                } catch (InterruptedException e) {
                    //ignore error
                }
            }
            recordElapsedTime("order writeDirectly end :");
            localResult.done();
            recordElapsedTime("order read start :");
            while (true) {
                if (terminate.get()) {
                    return;
                }
                RowDataPacket row = localResult.next();
                if (row == null) {
                    break;
                }
                if (nextHandler.rowResponse(null, row, this.isLeft, service))
                    break;
            }
            recordElapsedTime("order read end:");
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(null, this.isLeft, service);
        } catch (Exception e) {
            String msg = "OrderBy thread error, " + e.getLocalizedMessage();
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
    protected void terminateThread() throws Exception {
        this.queue.clear();
        this.queue.add(new RowDataPacket(0));
    }

    @Override
    protected void recycleResources() {
        this.queue.clear();
        if (this.localResult != null)
            this.localResult.close();
    }

}
