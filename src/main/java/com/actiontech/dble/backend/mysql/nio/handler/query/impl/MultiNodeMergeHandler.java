/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.ArrayMinHeap;
import com.actiontech.dble.backend.mysql.nio.handler.util.HeapItem;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * mergeHandler will merge data,if contains aggregate function,use group by handler
 *
 * @author ActionTech
 */
public class MultiNodeMergeHandler extends OwnThreadDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(MultiNodeMergeHandler.class);

    private final int queueSize;
    private final ReentrantLock lock;
    private List<BaseSelectHandler> exeHandlers;
    // map;conn->blocking queue.if receive row packet, add to the queue,if receive rowEof packet, add NullHeapItem into queue;
    private Map<MySQLConnection, BlockingQueue<HeapItem>> queues;
    private List<Order> orderBys;
    private RowDataComparator rowComparator;
    private RouteResultsetNode[] route;
    private int reachedConCount;
    private boolean isEasyMerge;

    public MultiNodeMergeHandler(long id, RouteResultsetNode[] route, boolean autocommit, NonBlockingSession session,
                                 List<Order> orderBys) {
        super(id, session);
        this.exeHandlers = new ArrayList<>();
        this.lock = new ReentrantLock();
        if (route.length == 0)
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "can not execute empty rrss!");
        for (RouteResultsetNode rrss : route) {
            BaseSelectHandler exeHandler = new BaseSelectHandler(id, rrss, autocommit, session);
            exeHandler.setNextHandler(this);
            this.exeHandlers.add(exeHandler);
        }
        this.route = route;
        this.orderBys = orderBys;
        this.queueSize = DbleServer.getInstance().getConfig().getSystem().getMergeQueueSize();
        this.isEasyMerge = route.length == 1 || (orderBys == null || orderBys.size() == 0);
        this.queues = new ConcurrentHashMap<>();
        this.merges.add(this);
    }

    public List<BaseSelectHandler> getExeHandlers() {
        return exeHandlers;
    }

    public RouteResultsetNode[] getRouteSources() {
        return this.route;
    }

    public void execute() throws Exception {
        synchronized (exeHandlers) {
            if (terminate.get())
                return;
            for (BaseSelectHandler exeHandler : exeHandlers) {
                MySQLConnection exeConn = exeHandler.initConnection();
                if (exeConn != null) {
                    exeConn.setComplexQuery(true);
                    BlockingQueue<HeapItem> queue = new LinkedBlockingQueue<>(queueSize);
                    queues.put(exeConn, queue);
                    exeHandler.execute(exeConn);
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(conn.toString() + "'s field is reached.");
        }
        // if terminated
        if (terminate.get()) {
            return;
        }
        lock.lock(); // for combine
        try {
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                rowComparator = makeRowDataSorter((MySQLConnection) conn);
                nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
            }
            if (!isEasyMerge) {
                if (++reachedConCount == route.length) {
                    startOwnThread();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;

        if (isEasyMerge) {
            nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
        } else {
            BlockingQueue<HeapItem> queue = queues.get(conn);
            if (queue == null)
                return true;
            HeapItem item = new HeapItem(row, rowPacket, (MySQLConnection) conn);
            try {
                queue.put(item);
            } catch (InterruptedException e) {
                //ignore error
            }
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(conn.toString() + " 's rowEof is reached.");
        }
        ((MySQLConnection) conn).setRunning(false);
        if (this.terminate.get())
            return;
        if (isEasyMerge) {
            lock.lock();
            try {
                if (++reachedConCount == route.length)
                    nextHandler.rowEofResponse(null, this.isLeft, conn);
            } finally {
                lock.unlock();
            }
        } else {
            BlockingQueue<HeapItem> queue = queues.get(conn);
            if (queue == null)
                return;
            try {
                queue.put(HeapItem.nullItem());
            } catch (InterruptedException e) {
                //ignore error
            }
        }
    }

    @Override
    protected void ownThreadJob(Object... objects) {
        try {
            ArrayMinHeap<HeapItem> heap = new ArrayMinHeap<>(new Comparator<HeapItem>() {

                @Override
                public int compare(HeapItem o1, HeapItem o2) {
                    RowDataPacket row1 = o1.getRowPacket();
                    RowDataPacket row2 = o2.getRowPacket();
                    if (row1 == null || row2 == null) {
                        if (row1 == row2)
                            return 0;
                        if (row1 == null)
                            return -1;
                        return 1;
                    }
                    return rowComparator.compare(row1, row2);
                }
            });
            // init heap
            for (Map.Entry<MySQLConnection, BlockingQueue<HeapItem>> entry : queues.entrySet()) {
                HeapItem firstItem = entry.getValue().take();
                heap.add(firstItem);
            }
            boolean filterFinished = false;
            while (!heap.isEmpty()) {
                if (terminate.get())
                    return;
                HeapItem top = heap.peak();
                if (top.isNullItem()) {
                    heap.poll();
                } else {
                    BlockingQueue<HeapItem> topitemQueue = queues.get(top.getIndex());
                    HeapItem item = topitemQueue.take();
                    heap.replaceTop(item);
                    if (filterFinished) {
                        continue;
                    }
                    if (nextHandler.rowResponse(top.getRowData(), top.getRowPacket(), this.isLeft, top.getIndex())) {
                        filterFinished = true;
                    }
                }
            }
            if (LOGGER.isInfoEnabled()) {
                String executeSqls = getRoutesSql(route);
                LOGGER.info(executeSqls + " heap send eof: ");
            }
            nextHandler.rowEofResponse(null, this.isLeft, queues.keySet().iterator().next());
        } catch (Exception e) {
            String msg = "Merge thread error, " + e.getLocalizedMessage();
            LOGGER.warn(msg, e);
            session.onQueryError(msg.getBytes());
        }
    }

    @Override
    protected void terminateThread() throws Exception {
        for (Entry<MySQLConnection, BlockingQueue<HeapItem>> entry : this.queues.entrySet()) {
            // add EOF to signal atoMerge thread
            entry.getValue().clear();
            entry.getValue().put(new HeapItem(null, null, entry.getKey()));
        }
        synchronized (exeHandlers) {
            for (BaseSelectHandler exeHandler : exeHandlers) {
                terminatePreHandler(exeHandler);
            }
        }
    }

    @Override
    protected void recycleResources() {
        synchronized (exeHandlers) {
            if (!terminate.get()) {
                for (BaseSelectHandler exeHandler : exeHandlers) {
                    terminatePreHandler(exeHandler);
                }
            }
        }
        Iterator<Entry<MySQLConnection, BlockingQueue<HeapItem>>> iter = this.queues.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<MySQLConnection, BlockingQueue<HeapItem>> entry = iter.next();
            // fair lock queue,poll for clear
            while (entry.getValue().poll() != null) {
                //do nothing
            }
            iter.remove();
        }
    }

    /**
     * s
     *
     * @param handler
     */
    private void terminatePreHandler(DMLResponseHandler handler) {
        DMLResponseHandler current = handler;
        while (current != null) {
            if (current == this)
                break;
            current.terminate();
            current = current.getNextHandler();
        }
    }

    private RowDataComparator makeRowDataSorter(MySQLConnection conn) {
        if (!isEasyMerge)
            return new RowDataComparator(this.fieldPackets, orderBys, this.isAllPushDown(), this.type());
        return null;
    }

    @Override
    public HandlerType type() {
        return HandlerType.MERGE;
    }

    private String getRoutesSql(RouteResultsetNode[] nodes) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        Map<String, List<RouteResultsetNode>> sqlMap = new HashMap<>();
        for (RouteResultsetNode rrss : nodes) {
            String sql = rrss.getStatement();
            if (!sqlMap.containsKey(sql)) {
                List<RouteResultsetNode> rrssList = new ArrayList<>();
                rrssList.add(rrss);
                sqlMap.put(sql, rrssList);
            } else {
                List<RouteResultsetNode> rrssList = sqlMap.get(sql);
                rrssList.add(rrss);
            }
        }
        for (Map.Entry<String, List<RouteResultsetNode>> entry : sqlMap.entrySet()) {
            sb.append(entry.getKey()).append(entry.getValue()).append(';');
        }
        sb.append('}');
        return sb.toString();
    }
}
