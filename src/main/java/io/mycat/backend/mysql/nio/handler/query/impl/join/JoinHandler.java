package io.mycat.backend.mysql.nio.handler.query.impl.join;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.nio.handler.util.TwoTableComparator;
import io.mycat.backend.mysql.store.LocalResult;
import io.mycat.backend.mysql.store.UnSortedLocalResult;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.FairLinkedBlockingDeque;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * join的策略目前为sortmerge，在merge数据到达之前，已经按照merge列进行了排序
 *
 * @author ActionTech
 */
public class JoinHandler extends OwnThreadDMLHandler {
    protected Logger logger = Logger.getLogger(JoinHandler.class);

    protected boolean isLeftJoin = false;
    protected FairLinkedBlockingDeque<LocalResult> leftQueue;
    protected FairLinkedBlockingDeque<LocalResult> rightQueue;
    protected List<Order> leftOrders;
    protected List<Order> rightOrders;
    protected List<FieldPacket> leftFieldPackets;
    protected List<FieldPacket> rightFieldPackets;
    private AtomicBoolean fieldSent = new AtomicBoolean(false);
    private BufferPool pool;
    private RowDataComparator leftCmptor;
    private RowDataComparator rightCmptor;
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

    public JoinHandler(long id, NonBlockingSession session, boolean isLeftJoin, List<Order> leftOrder,
                       List<Order> rightOrder, Item otherJoinOn) {
        super(id, session);
        this.isLeftJoin = isLeftJoin;
        this.leftOrders = leftOrder;
        this.rightOrders = rightOrder;
        int queueSize = MycatServer.getInstance().getConfig().getSystem().getJoinQueueSize();
        this.leftQueue = new FairLinkedBlockingDeque<LocalResult>(queueSize);
        this.rightQueue = new FairLinkedBlockingDeque<LocalResult>(queueSize);
        this.leftFieldPackets = new ArrayList<FieldPacket>();
        this.rightFieldPackets = new ArrayList<FieldPacket>();
        this.otherJoinOn = otherJoinOn;
    }

    @Override
    public HandlerType type() {
        return HandlerType.JOIN;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, final BackendConnection conn) {
        if (this.pool == null)
            this.pool = MycatServer.getInstance().getBufferPool();

        if (isLeft) {
            // logger.debug("field eof left");
            leftFieldPackets = fieldPackets;
            leftCmptor = new RowDataComparator(leftFieldPackets, leftOrders, this.isAllPushDown(), this.type(),
                    conn.getCharset());
        } else {
            // logger.debug("field eof right");
            rightFieldPackets = fieldPackets;
            rightCmptor = new RowDataComparator(rightFieldPackets, rightOrders, this.isAllPushDown(), this.type(),
                    conn.getCharset());
        }
        if (!fieldSent.compareAndSet(false, true)) {
            this.charset = conn.getCharset();
            List<FieldPacket> newFieldPacket = new ArrayList<FieldPacket>();
            newFieldPacket.addAll(leftFieldPackets);
            newFieldPacket.addAll(rightFieldPackets);
            nextHandler.fieldEofResponse(null, null, newFieldPacket, null, this.isLeft, conn);
            otherJoinOnItem = makeOtherJoinOnItem(newFieldPacket, conn);
            // logger.debug("all ready");
            startOwnThread(conn);
        }
    }

    /* 用来处理otherjoinonfilter的 */
    private Item makeOtherJoinOnItem(List<FieldPacket> rowpackets, BackendConnection conn) {
        this.joinRowFields = HandlerTool.createFields(rowpackets);
        if (otherJoinOn == null)
            return null;
        Item ret = HandlerTool.createItem(this.otherJoinOn, this.joinRowFields, 0, this.isAllPushDown(), this.type(),
                conn.getCharset());
        return ret;
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        logger.debug("rowresponse");
        if (terminate.get()) {
            return true;
        }
        try {
            if (isLeft) {
                leftLock.lock();
                try {
                    addRowToDeque(rowPacket, leftFieldPackets.size(), leftQueue, leftCmptor);
                } finally {
                    leftLock.unlock();
                }
            } else {
                rightLock.lock();
                try {
                    addRowToDeque(rowPacket, rightFieldPackets.size(), rightQueue, rightCmptor);
                } finally {
                    rightLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            logger.error("join row response exception", e);
            return true;
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        logger.debug("roweof");
        if (terminate.get()) {
            return;
        }
        RowDataPacket eofRow = new RowDataPacket(0);
        try {
            if (isLeft) {
                logger.debug("row eof left");
                addRowToDeque(eofRow, leftFieldPackets.size(), leftQueue, leftCmptor);
            } else {
                logger.debug("row eof right");
                addRowToDeque(eofRow, rightFieldPackets.size(), rightQueue, rightCmptor);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        MySQLConnection conn = (MySQLConnection) objects[0];
        LocalResult leftLocal = null, rightLocal = null;
        try {
            Comparator<RowDataPacket> joinCmptor = new TwoTableComparator(leftFieldPackets, rightFieldPackets,
                    leftOrders, rightOrders, this.isAllPushDown(), this.type(), conn.getCharset());

            // logger.debug("merge Join start");
            leftLocal = takeFirst(leftQueue);
            rightLocal = takeFirst(rightQueue);
            while (true) {
                if (terminate.get())
                    return;
                RowDataPacket leftRow = leftLocal.getLastRow();
                RowDataPacket rightRow = rightLocal.getLastRow();
                if (leftRow.fieldCount == 0) {
                    break;
                }
                if (rightRow.fieldCount == 0) {
                    if (isLeftJoin) {
                        if (connectLeftAndNull(leftLocal, conn))
                            break;
                        leftLocal = takeFirst(leftQueue);
                        continue;
                    } else {
                        break;
                    }
                }
                int rs = joinCmptor.compare(leftRow, rightRow);
                if (rs < 0) {
                    if (isLeftJoin) {
                        if (connectLeftAndNull(leftLocal, conn))
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
                    if (connectLeftAndRight(leftLocal, rightLocal, conn))
                        break;
                    leftLocal = takeFirst(leftQueue);
                    rightLocal = takeFirst(rightQueue);
                }
            }
            nextHandler.rowEofResponse(null, isLeft, conn);
            HandlerTool.terminateHandlerTree(this);
        } catch (Exception e) {
            String msg = "join thread error, " + e.getLocalizedMessage();
            logger.error(msg, e);
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
         * 前提条件是这个方法是个单线程
         */
        deque.waitUtilCount(1);
        LocalResult result = deque.peekFirst();
        RowDataPacket lastRow = result.getLastRow();
        if (lastRow.fieldCount == 0)
            return deque.takeFirst();
        else {
            deque.waitUtilCount(2);
            return deque.takeFirst();
        }
    }

    /**
     * @param leftRows
     * @param rightRows
     * @param conn
     * @return if is interrupted by next handler ,return true,else false
     * @throws Exception
     */
    private boolean connectLeftAndRight(LocalResult leftRows, LocalResult rightRows, MySQLConnection conn)
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
                    if (nextHandler.rowResponse(null, rowPacket, isLeft, conn))
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
                    if (nextHandler.rowResponse(null, rowPacket, isLeft, conn))
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

    private boolean connectLeftAndNull(LocalResult leftRows, MySQLConnection conn) throws Exception {
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
                if (nextHandler.rowResponse(null, rowPacket, isLeft, conn))
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
            if (lastRow.fieldCount == 0) {
                // 有可能是terminateThread添加的eof
                return;
            } else if (row.fieldCount > 0 && cmp.compare(lastRow, row) == 0) {
                localResult.add(row);
                return;
            } else {
                localResult.done();
            }
        }
        LocalResult newLocalResult = new UnSortedLocalResult(columnCount, pool, this.charset).
                setMemSizeController(session.getJoinBufferMC());
        newLocalResult.add(row);
        if (row.fieldCount == 0)
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
