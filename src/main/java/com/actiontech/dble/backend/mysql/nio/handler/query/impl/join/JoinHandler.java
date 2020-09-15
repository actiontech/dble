/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.join;


import com.actiontech.dble.backend.mysql.CharsetUtil;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.nio.handler.util.TwoTableComparator;
import com.actiontech.dble.backend.mysql.store.LocalResult;
import com.actiontech.dble.backend.mysql.store.UnSortedLocalResult;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.util.FairLinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * join strategy is sortMerge,the merge data has been ordered
 *
 * @author ActionTech
 */
public class JoinHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinHandler.class);

    protected boolean isLeftJoin = false;
    protected FairLinkedBlockingDeque<LocalResult> leftQueue;
    protected FairLinkedBlockingDeque<LocalResult> rightQueue;
    protected List<Order> leftOrders;
    protected List<Order> rightOrders;
    protected List<FieldPacket> leftFieldPackets;
    protected List<FieldPacket> rightFieldPackets;
    private AtomicBoolean fieldSent = new AtomicBoolean(false);
    private BufferPool pool;
    private RowDataComparator leftComparator;
    private RowDataComparator rightComparator;
    // @bug 1097
    // only join columns same is not enough
    private List<Field> joinRowFields;
    private Item otherJoinOn;
    private Item otherJoinOnItem;
    // @bug 1208
    private String charset = "UTF-8";
    // prevent multi thread rowresponse
    protected ReentrantLock leftLock = new ReentrantLock();
    protected ReentrantLock rightLock = new ReentrantLock();

    public JoinHandler(long id, Session session, boolean isLeftJoin, List<Order> leftOrder,
                       List<Order> rightOrder, Item otherJoinOn) {
        super(id, session);
        this.isLeftJoin = isLeftJoin;
        this.leftOrders = leftOrder;
        this.rightOrders = rightOrder;
        int queueSize = SystemConfig.getInstance().getJoinQueueSize();
        this.leftQueue = new FairLinkedBlockingDeque<>(queueSize);
        this.rightQueue = new FairLinkedBlockingDeque<>(queueSize);
        this.leftFieldPackets = new ArrayList<>();
        this.rightFieldPackets = new ArrayList<>();
        this.otherJoinOn = otherJoinOn;
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
        this.charset = service != null ? CharsetUtil.getJavaCharset(service.getConnection().getCharsetName().getResults()) : CharsetUtil.getJavaCharset(session.getSource().getCharsetName().getResults());
        if (!fieldSent.compareAndSet(false, true)) {
            List<FieldPacket> newFieldPacket = new ArrayList<>();
            newFieldPacket.addAll(leftFieldPackets);
            newFieldPacket.addAll(rightFieldPackets);
            nextHandler.fieldEofResponse(null, null, newFieldPacket, null, this.isLeft, service);
            otherJoinOnItem = makeOtherJoinOnItem(newFieldPacket);
            // logger.debug("all ready");
            startOwnThread(service);
        }
    }

    private Item makeOtherJoinOnItem(List<FieldPacket> rowPackets) {
        this.joinRowFields = HandlerTool.createFields(rowPackets);
        if (otherJoinOn == null)
            return null;
        return HandlerTool.createItem(this.otherJoinOn, this.joinRowFields, 0, this.isAllPushDown(), this.type());
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService conn) {
        LOGGER.debug("rowresponse");
        if (terminate.get()) {
            return true;
        }
        try {
            if (isLeft) {
                leftLock.lock();
                try {
                    addRowToDeque(rowPacket, leftFieldPackets.size(), leftQueue, leftComparator);
                } finally {
                    leftLock.unlock();
                }
            } else {
                rightLock.lock();
                try {
                    addRowToDeque(rowPacket, rightFieldPackets.size(), rightQueue, rightComparator);
                } finally {
                    rightLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            LOGGER.info("join row response exception", e);
            return true;
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        LOGGER.debug("roweof");
        if (terminate.get()) {
            return;
        }
        RowDataPacket eofRow = new RowDataPacket(0);
        try {
            if (isLeft) {
                LOGGER.debug("row eof left");
                addRowToDeque(eofRow, leftFieldPackets.size(), leftQueue, leftComparator);
            } else {
                LOGGER.debug("row eof right");
                addRowToDeque(eofRow, rightFieldPackets.size(), rightQueue, rightComparator);
            }
        } catch (InterruptedException e) {
            LOGGER.warn("JoinHandler rowEofResponse InterruptedException ", e);
        }
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        MySQLResponseService service = (MySQLResponseService) objects[0];
        LocalResult leftLocal = null, rightLocal = null;
        try {
            Comparator<RowDataPacket> joinComparator = new TwoTableComparator(leftFieldPackets, rightFieldPackets,
                    leftOrders, rightOrders, this.isAllPushDown(), this.type(), CharsetUtil.getCollationIndex(session.getSource().getCharsetName().getCollation()));

            // logger.debug("merge Join start");
            leftLocal = takeFirst(leftQueue);
            rightLocal = takeFirst(rightQueue);
            while (true) {
                if (terminate.get())
                    return;
                RowDataPacket leftRow = leftLocal.getLastRow();
                RowDataPacket rightRow = rightLocal.getLastRow();
                if (leftRow.getFieldCount() == 0) {
                    break;
                }
                if (rightRow.getFieldCount() == 0) {
                    if (isLeftJoin) {
                        if (connectLeftAndNull(leftLocal, service))
                            break;
                        leftLocal = takeFirst(leftQueue);
                        continue;
                    } else {
                        break;
                    }
                }
                int rs = joinComparator.compare(leftRow, rightRow);
                if (rs < 0) {
                    if (isLeftJoin) {
                        if (connectLeftAndNull(leftLocal, service))
                            break;
                        leftLocal = takeFirst(leftQueue);
                        continue;
                    } else {
                        leftLocal.close();
                        leftLocal = takeFirst(leftQueue);
                    }
                } else if (rs > 0) {
                    rightLocal.close();
                    rightLocal = takeFirst(rightQueue);
                } else {
                    if (connectLeftAndRight(leftLocal, rightLocal, service))
                        break;
                    leftLocal = takeFirst(leftQueue);
                    rightLocal = takeFirst(rightQueue);
                }
            }

            HandlerTool.terminateHandlerTree(this);
            // for trace, when join end before all rows return ,the handler should mark as finished
            for (DMLResponseHandler mergeHandler : this.getMerges()) {
                DMLResponseHandler handler = mergeHandler;
                while (handler != null && handler != this) {
                    session.setHandlerEnd(handler);
                    handler = handler.getNextHandler();
                }
            }
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(null, isLeft, service);
        } catch (Exception e) {
            String msg = "join thread error, " + e.getLocalizedMessage();
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
        /**
         * it must be in single thread
         */
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

    /**
     * @param leftRows
     * @param rightRows
     * @return if is interrupted by next handler ,return true,else false
     * @throws Exception
     */
    private boolean connectLeftAndRight(LocalResult leftRows, LocalResult rightRows, MySQLResponseService service)
            throws Exception {
        RowDataPacket leftRow = null;
        RowDataPacket rightRow = null;
        try {
            while ((leftRow = leftRows.next()) != null) {
                // @bug 1097
                int matchCount = 0;
                while ((rightRow = rightRows.next()) != null) {
                    RowDataPacket rowPacket = new RowDataPacket(leftFieldPackets.size() + rightFieldPackets.size());
                    for (byte[] value : leftRow.fieldValues) {
                        rowPacket.add(value);
                    }
                    for (byte[] value : rightRow.fieldValues) {
                        rowPacket.add(value);
                    }
                    if (otherJoinOnItem != null) {
                        HandlerTool.initFields(joinRowFields, rowPacket.fieldValues);
                        if (!otherJoinOnItem.valBool())
                            continue;
                    }
                    matchCount++;
                    if (nextHandler.rowResponse(null, rowPacket, isLeft, service))
                        return true;
                }
                // @bug 1097
                // condition: exist otherOnItem and no row match other condition
                // send left row and null
                if (matchCount == 0 && isLeftJoin) {
                    RowDataPacket rowPacket = new RowDataPacket(leftFieldPackets.size() + rightFieldPackets.size());
                    for (byte[] value : leftRow.fieldValues) {
                        rowPacket.add(value);
                    }
                    for (int i = 0; i < rightFieldPackets.size(); i++) {
                        rowPacket.add(null);
                    }
                    if (nextHandler.rowResponse(null, rowPacket, isLeft, service))
                        return true;
                }
                rightRows.reset();
            }
            return false;
        } finally {
            leftRows.close();
            rightRows.close();
        }
    }

    private boolean connectLeftAndNull(LocalResult leftRows, MySQLResponseService service) throws Exception {
        RowDataPacket leftRow = null;
        try {
            while ((leftRow = leftRows.next()) != null) {
                RowDataPacket rowPacket = new RowDataPacket(leftFieldPackets.size() + rightFieldPackets.size());
                for (byte[] value : leftRow.fieldValues) {
                    rowPacket.add(value);
                }
                for (int i = 0; i < rightFieldPackets.size(); i++) {
                    rowPacket.add(null);
                }
                if (nextHandler.rowResponse(null, rowPacket, isLeft, service))
                    return true;
            }
            return false;
        } finally {
            leftRows.close();
        }
    }

    private void addRowToDeque(RowDataPacket row, int columnCount, FairLinkedBlockingDeque<LocalResult> deque,
                               RowDataComparator cmp) throws InterruptedException {
        LocalResult localResult = deque.peekLast();
        if (localResult != null) {
            RowDataPacket lastRow = localResult.getLastRow();
            if (lastRow.getFieldCount() == 0) {
                // eof may added in terminateThread
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
