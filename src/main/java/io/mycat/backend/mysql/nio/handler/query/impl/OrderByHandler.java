package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.LocalResult;
import io.mycat.backend.mysql.store.SortedLocalResult;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.TimeUtil;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class OrderByHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(OrderByHandler.class);

    private List<Order> orders;
    private RowDataComparator cmp = null;
    private BlockingQueue<RowDataPacket> queue;
    /* 排序对象，支持缓存、文件系统 */
    private LocalResult localResult;
    private BufferPool pool;
    private int queueSize;

    public OrderByHandler(long id, NonBlockingSession session, List<Order> orders) {
        super(id, session);
        this.orders = orders;
        this.queueSize = MycatServer.getInstance().getConfig().getSystem().getOrderByQueueSize();
        this.queue = new LinkedBlockingDeque<RowDataPacket>(queueSize);
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
            this.pool = MycatServer.getInstance().getBufferPool();

        this.fieldPackets = fieldPackets;
        cmp = new RowDataComparator(this.fieldPackets, orders, isAllPushDown(), type(), conn.getCharset());
        localResult = new SortedLocalResult(pool, fieldPackets.size(), cmp, conn.getCharset()).
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
                if (row.fieldCount == 0) {
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
