/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.util.ArrayMinHeap;
import com.actiontech.dble.backend.mysql.nio.handler.util.HeapItem;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * mergeHandler will merge data,if contains aggregate function,use group by handler
 *
 * @author ActionTech
 */
public class MultiNodeMergeAndOrderHandler extends MultiNodeMergeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeMergeAndOrderHandler.class);

    private final int queueSize;
    // map;conn->blocking queue.if receive row packet, add to the queue,if receive rowEof packet, add NullHeapItem into queue;
    private Map<MySQLResponseService, BlockingQueue<HeapItem>> queues;
    private List<Order> orderBys;
    private RowDataComparator rowComparator;
    private volatile boolean noNeedRows = false;

    public MultiNodeMergeAndOrderHandler(long id, RouteResultsetNode[] route, boolean autocommit, NonBlockingSession session,
                                         List<Order> orderBys) {
        super(id, route, autocommit, session);
        this.orderBys = orderBys;
        this.queueSize = SystemConfig.getInstance().getMergeQueueSize();
        this.queues = new ConcurrentHashMap<>();
        this.merges.add(this);

    }

    @Override
    public void execute() {
        synchronized (exeHandlers) {
            if (terminate.get())
                return;

            if (Thread.currentThread().getName().contains("complexQueryExecutor")) {
                doExecute();
            } else {
                DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        doExecute();
                    }
                });
            }
        }
    }

    private void doExecute() {
        for (BaseSelectHandler exeHandler : exeHandlers) {
            session.setHandlerStart(exeHandler); //base start execute
            try {
                BackendConnection exeConn = exeHandler.initConnection();
                exeConn.getBackendService().setComplexQuery(true);
                queues.put(exeConn.getBackendService(), new LinkedBlockingQueue<>(queueSize));
                exeHandler.execute(exeConn.getBackendService());
            } catch (Exception e) {
                exeHandler.connectionError(e, exeHandler.getRrss());
                return;
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service.toString() + "'s field is reached.");
        }
        session.setHandlerStart(this);
        // if terminated
        if (terminate.get()) {
            return;
        }
        lock.lock(); // for combine
        try {
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                rowComparator = new RowDataComparator(this.fieldPackets, orderBys, this.isAllPushDown(), this.type());
                nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
            }
            if (++reachedConCount == route.length) {
                session.allBackendConnReceive();
                startOwnThread();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        if (terminate.get() || noNeedRows)
            return true;

        MySQLResponseService mySQLConn = (MySQLResponseService) service;
        BlockingQueue<HeapItem> queue = queues.get(mySQLConn);
        if (queue == null)
            return true;
        HeapItem item = new HeapItem(row, rowPacket, mySQLConn);
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            //ignore error
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        AbstractService responseService = (MySQLResponseService) service;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(responseService.toString() + " 's rowEof is reached.");
        }

        if (this.terminate.get())
            return;
        BlockingQueue<HeapItem> queue = queues.get(responseService);
        if (queue == null)
            return;
        try {
            queue.put(HeapItem.nullItem());
        } catch (InterruptedException e) {
            //ignore error
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
            for (Entry<MySQLResponseService, BlockingQueue<HeapItem>> entry : queues.entrySet()) {
                HeapItem firstItem = entry.getValue().take();
                heap.add(firstItem);
            }
            while (!heap.isEmpty()) {
                if (terminate.get())
                    return;
                HeapItem top = heap.peak();
                if (top.isNullItem()) {
                    heap.poll();
                } else {
                    BlockingQueue<HeapItem> topItemQueue = queues.get(top.getIndex());
                    HeapItem item = topItemQueue.take();
                    heap.replaceTop(item);
                    if (nextHandler.rowResponse(top.getRowData(), top.getRowPacket(), this.isLeft, top.getIndex())) {
                        noNeedRows = true;
                        while (!heap.isEmpty()) {
                            HeapItem itemToDiscard = heap.poll();
                            if (!itemToDiscard.isNullItem()) {
                                BlockingQueue<HeapItem> discardQueue = queues.get(itemToDiscard.getIndex());
                                while (true) {
                                    if (discardQueue.take().isNullItem() || terminate.get()) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (LOGGER.isDebugEnabled()) {
                String executeQueries = getRoutesSql(route);
                LOGGER.debug(executeQueries + " heap send eof: ");
            }
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(null, this.isLeft, queues.keySet().iterator().next());
        } catch (Exception e) {
            String msg = "Merge thread error, " + e.getLocalizedMessage();
            LOGGER.info(msg, e);
            session.onQueryError(msg.getBytes());
        }
    }

    @Override
    protected void terminateThread() throws Exception {
        for (Entry<MySQLResponseService, BlockingQueue<HeapItem>> entry : this.queues.entrySet()) {
            // add EOF to signal atoMerge thread
            entry.getValue().clear();
            entry.getValue().put(new HeapItem(null, null, entry.getKey()));
        }
        recycleConn();
    }

    @Override
    protected void recycleResources() {
        Iterator<Entry<MySQLResponseService, BlockingQueue<HeapItem>>> iterator = this.queues.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<MySQLResponseService, BlockingQueue<HeapItem>> entry = iterator.next();
            // fair lock queue,poll for clear
            while (true) {
                if (entry.getValue().poll() == null) {
                    break;
                }
            }
            iterator.remove();
        }
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
        for (Entry<String, List<RouteResultsetNode>> entry : sqlMap.entrySet()) {
            sb.append(entry.getKey()).append(entry.getValue()).append(';');
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public HandlerType type() {
        return HandlerType.MERGE_AND_ORDER;
    }
}
