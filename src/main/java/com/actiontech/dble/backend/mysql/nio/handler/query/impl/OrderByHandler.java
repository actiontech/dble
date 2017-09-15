/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.LocalResult;
import com.actiontech.dble.backend.mysql.store.SortedLocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.TimeUtil;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class OrderByHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(OrderByHandler.class);

    private List<Order> orders;
    private BlockingQueue<RowDataPacket> queue;
    /* tmp object for ordering,support Memory-mapped file or file */
    private LocalResult localResult;
    private BufferPool pool;

    public OrderByHandler(long id, NonBlockingSession session, List<Order> orders) {
        super(id, session);
        this.orders = orders;
        int queueSize = DbleServer.getInstance().getConfig().getSystem().getOrderByQueueSize();
        this.queue = new LinkedBlockingDeque<>(queueSize);
    }

    @Override
    public HandlerType type() {
        return HandlerType.ORDERBY;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, final BackendConnection conn) {
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = DbleServer.getInstance().getBufferPool();

        this.fieldPackets = fieldPackets;
        RowDataComparator cmp = new RowDataComparator(this.fieldPackets, orders, isAllPushDown(), type());
        localResult = new SortedLocalResult(pool, fieldPackets.size(), cmp, CharsetUtil.getJavaCharset(conn.getCharset().getResults())).
                setMemSizeController(session.getOrderBufferMC());
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
        startOwnThread(conn);
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
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
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
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
        MySQLConnection conn = (MySQLConnection) objects[0];
        recordElapsedTime("order write start :");
        try {
            while (true) {
                if (terminate.get()) {
                    return;
                }
                RowDataPacket row = null;
                try {
                    row = queue.take();
                } catch (InterruptedException e) {
                    //ignore error
                }
                if (row.getFieldCount() == 0) {
                    break;
                }
                localResult.add(row);
            }
            recordElapsedTime("order write end :");
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
                if (nextHandler.rowResponse(null, row, this.isLeft, conn))
                    break;
            }
            recordElapsedTime("order read end:");
            nextHandler.rowEofResponse(null, this.isLeft, conn);
        } catch (Exception e) {
            String msg = "OrderBy thread error, " + e.getLocalizedMessage();
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
