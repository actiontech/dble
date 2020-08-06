/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.join;


import com.actiontech.dble.backend.mysql.CharsetUtil;

import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.nio.handler.util.TwoTableComparator;
import com.actiontech.dble.backend.mysql.store.LocalResult;
import com.actiontech.dble.backend.mysql.store.UnSortedLocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.util.FairLinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class NotInHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NotInHandler.class);

    private FairLinkedBlockingDeque<LocalResult> leftQueue;
    private FairLinkedBlockingDeque<LocalResult> rightQueue;
    private List<Order> leftOrders;
    private List<Order> rightOrders;
    private List<FieldPacket> leftFieldPackets;
    private List<FieldPacket> rightFieldPackets;
    private BufferPool pool;
    private RowDataComparator leftComparator;
    private RowDataComparator rightComparator;
    private AtomicBoolean fieldSent = new AtomicBoolean(false);
    private String charset = "UTF-8";

    public NotInHandler(long id, Session session, List<Order> leftOrder, List<Order> rightOrder) {
        super(id, session);
        this.leftOrders = leftOrder;
        this.rightOrders = rightOrder;
        int queueSize = SystemConfig.getInstance().getJoinQueueSize();
        this.leftQueue = new FairLinkedBlockingDeque<>(queueSize);
        this.rightQueue = new FairLinkedBlockingDeque<>(queueSize);
        this.leftFieldPackets = new ArrayList<>();
        this.rightFieldPackets = new ArrayList<>();
    }

    @Override
    public HandlerType type() {
        return HandlerType.JOIN;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, final AbstractService service) {
        session.setHandlerStart(this);
        if (this.pool == null)
            this.pool = BufferPoolManager.getBufferPool();

        if (isLeft) {
            // logger.debug("field eof left");
            leftFieldPackets = fieldPackets;
            leftComparator = new RowDataComparator(leftFieldPackets, leftOrders, this.isAllPushDown(), this.type());
        } else {
            // logger.debug("field eof right");
            rightFieldPackets = fieldPackets;
            rightComparator = new RowDataComparator(rightFieldPackets, rightOrders, this.isAllPushDown(), this.type());
        }
        if (!fieldSent.compareAndSet(false, true)) {
            this.charset = service != null ? CharsetUtil.getJavaCharset(service.getConnection().getCharsetName().getResults()) : CharsetUtil.getJavaCharset(session.getSource().getCharsetName().getResults());
            nextHandler.fieldEofResponse(null, null, leftFieldPackets, null, this.isLeft, service);
            // logger.debug("all ready");
            startOwnThread(service);
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        LOGGER.debug("rowresponse");
        if (terminate.get()) {
            return true;
        }
        try {
            if (isLeft) {
                addRowToDeque(rowPacket, leftFieldPackets.size(), leftQueue, leftComparator);
            } else {
                addRowToDeque(rowPacket, rightFieldPackets.size(), rightQueue, rightComparator);
            }
        } catch (InterruptedException e) {
            LOGGER.info("not in row exception", e);
            return true;
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        LOGGER.info("roweof");
        if (terminate.get()) {
            return;
        }
        RowDataPacket eofRow = new RowDataPacket(0);
        try {
            if (isLeft) {
                // logger.debug("row eof left");
                addRowToDeque(eofRow, leftFieldPackets.size(), leftQueue, leftComparator);
            } else {
                // logger.debug("row eof right");
                addRowToDeque(eofRow, rightFieldPackets.size(), rightQueue, rightComparator);
            }
        } catch (Exception e) {
            LOGGER.info("not in rowEof exception", e);
        }
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        MySQLResponseService service = (MySQLResponseService) objects[0];
        LocalResult leftLocal = null, rightLocal = null;
        try {
            Comparator<RowDataPacket> notInComparator = new TwoTableComparator(leftFieldPackets, rightFieldPackets,
                    leftOrders, rightOrders, this.isAllPushDown(), this.type(), CharsetUtil.getCollationIndex(session.getSource().getCharsetName().getCollation()));

            leftLocal = takeFirst(leftQueue);
            rightLocal = takeFirst(rightQueue);
            while (true) {
                RowDataPacket leftRow = leftLocal.getLastRow();
                RowDataPacket rightRow = rightLocal.getLastRow();
                if (leftRow.getFieldCount() == 0) {
                    break;
                }
                if (rightRow.getFieldCount() == 0) {
                    sendLeft(leftLocal, service);
                    leftLocal.close();
                    leftLocal = takeFirst(leftQueue);
                    continue;
                }
                int rs = notInComparator.compare(leftRow, rightRow);
                if (rs < 0) {
                    sendLeft(leftLocal, service);
                    leftLocal.close();
                    leftLocal = takeFirst(leftQueue);
                    continue;
                } else if (rs > 0) {
                    rightLocal.close();
                    rightLocal = takeFirst(rightQueue);
                } else {
                    // because not in, if equal left should move to next value
                    leftLocal.close();
                    rightLocal.close();
                    leftLocal = takeFirst(leftQueue);
                    rightLocal = takeFirst(rightQueue);
                }
            }
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(null, isLeft, service);
            HandlerTool.terminateHandlerTree(this);
        } catch (Exception e) {
            String msg = "notIn thread error, " + e.getLocalizedMessage();
            LOGGER.info(msg, e);
            session.onQueryError(msg.getBytes());
        } finally {
            if (leftLocal != null)
                leftLocal.close();
            if (rightLocal != null)
                rightLocal.close();
        }
    }

    private LocalResult takeFirst(FairLinkedBlockingDeque<LocalResult> deque) throws InterruptedException {
        deque.waitUtilCount(1);
        LocalResult result = deque.peekFirst();
        RowDataPacket lastRow = result.getLastRow();
        if (lastRow.getFieldCount() == 0)
            return deque.takeFirst();
        else {
            deque.waitUtilCount(2);
            return deque.takeFirst();
        }
    }

    private void sendLeft(LocalResult leftRows, AbstractService service) throws Exception {
        RowDataPacket leftRow = null;
        while ((leftRow = leftRows.next()) != null) {
            nextHandler.rowResponse(null, leftRow, isLeft, service);
        }
    }

    private void addRowToDeque(RowDataPacket row, int columnCount, FairLinkedBlockingDeque<LocalResult> deque,
                               RowDataComparator cmp) throws InterruptedException {
        LocalResult localResult = deque.peekLast();
        if (localResult != null) {
            RowDataPacket lastRow = localResult.getLastRow();
            if (lastRow.getFieldCount() == 0) {
                return;
            } else if (row.getFieldCount() > 0 && cmp.compare(lastRow, row) == 0) {
                localResult.add(row);
                return;
            } else {
                localResult.done();
            }
        }
        LocalResult newLocalResult = new UnSortedLocalResult(columnCount, pool, this.charset).
                setMemSizeController(session.getJoinBufferMC());
        newLocalResult.add(row);
        if (row.getFieldCount() == 0)
            newLocalResult.done();
        deque.putLast(newLocalResult);
    }

    /**
     * only for terminate.
     *
     * @param row
     * @param columnCount
     * @param deque
     * @throws InterruptedException
     */
    private void addEndRowToDeque(RowDataPacket row, int columnCount, FairLinkedBlockingDeque<LocalResult> deque)
            throws InterruptedException {
        LocalResult newLocalResult = new UnSortedLocalResult(columnCount, pool, this.charset).
                setMemSizeController(session.getJoinBufferMC());
        newLocalResult.add(row);
        newLocalResult.done();
        LocalResult localResult = deque.addOrReplaceLast(newLocalResult);
        if (localResult != null)
            localResult.close();
    }

    @Override
    protected void terminateThread() throws Exception {
        RowDataPacket eofRow = new RowDataPacket(0);
        addEndRowToDeque(eofRow, leftFieldPackets.size(), leftQueue);
        RowDataPacket eofRow2 = new RowDataPacket(0);
        addEndRowToDeque(eofRow2, rightFieldPackets.size(), rightQueue);
    }

    @Override
    protected void recycleResources() {
        clearDeque(this.leftQueue);
        clearDeque(this.rightQueue);
    }

    private void clearDeque(FairLinkedBlockingDeque<LocalResult> deque) {
        if (deque == null)
            return;
        LocalResult local = deque.poll();
        while (local != null) {
            local.close();
            local = deque.poll();
        }
    }
}
