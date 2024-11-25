/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;

import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.backend.mysql.store.LocalResult;
import com.oceanbase.obsharding_d.backend.mysql.store.SortedLocalResult;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.singleton.BufferPoolManager;
import com.oceanbase.obsharding_d.util.TimeUtil;
import org.jetbrains.annotations.NotNull;
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
                                 byte[] eofNull, boolean isLeft, @NotNull final AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        if (this.pool == null)
            this.pool = BufferPoolManager.getBufferPool();

        this.fieldPackets = fieldPackets;
        RowDataComparator cmp = new RowDataComparator(this.fieldPackets, orders, isAllPushDown(), type());
        String charSet = !service.isFakeClosed() ? CharsetUtil.getJavaCharset(service.getCharset().getResults()) :
                CharsetUtil.getJavaCharset(session.getSource().getService().getCharset().getResults());
        localResult = new SortedLocalResult(pool, fieldPackets.size(), cmp, charSet, generateBufferRecordBuilder()).
                setMemSizeController(session.getOrderBufferMC());
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
        startOwnThread(service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
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
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        try {
            queue.put(TERMINATED_ROW);
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
        } catch (MySQLOutPutException e) {
            String msg = e.getLocalizedMessage();
            LOGGER.info(msg, e);
            session.onQueryError(msg.getBytes());
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
    protected void terminateThread() {
        this.queue.clear();
        this.queue.add(TERMINATED_ROW);
    }

    @Override
    protected void recycleResources() {
        this.queue.clear();
        if (this.localResult != null)
            this.localResult.close();
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.ORDER;
    }

}
