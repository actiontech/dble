/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.backend.mysql.nio.handler.util.ArrayMinHeap;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.HeapItem;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MultiNodeSelectHandler extends MultiNodeQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeSelectHandler.class);
    private final int queueSize;
    private Map<MySQLResponseService, BlockingQueue<HeapItem>> queues;
    private RowDataComparator rowComparator;
    private OutputHandler outputHandler;
    private volatile boolean noNeedRows = false;

    public MultiNodeSelectHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
        this.queueSize = SystemConfig.getInstance().getMergeQueueSize();
        this.queues = new ConcurrentHashMap<>();
        outputHandler = new OutputHandler(BaseHandlerBuilder.getSequenceId(), session);
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-response");
        TraceManager.finishSpan(service, traceObject);
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + service);
        }
        if (executeResponse) {
            String reason = "unexpected okResponse";
            LOGGER.info(reason);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        queues.put((MySQLResponseService) service, new LinkedBlockingQueue<>(queueSize));
        lock.lock();
        try {
            if (isFail()) {
                if (decrementToZero((MySQLResponseService) service)) {
                    session.resetMultiStatementStatus();
                    handleEndPacket(err, AutoTxOperation.ROLLBACK, false);
                }
            } else {
                if (!fieldsReturned) {
                    fieldsReturned = true;
                    mergeFieldEof(fields, (MySQLResponseService) service);
                }
                if (decrementToZero((MySQLResponseService) service)) {
                    startOwnThread();
                }
            }
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-rowEof-response");
        TraceManager.finishSpan(service, traceObject);
        BlockingQueue<HeapItem> queue = queues.get(service);
        if (queue == null)
            return;
        try {
            queue.put(HeapItem.nullItem());
        } catch (InterruptedException e) {
            LOGGER.info("rowEofResponse error", e);
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, AbstractService service) {
        if (errorResponse.get() || noNeedRows) {
            return true;
        }
        BlockingQueue<HeapItem> queue = queues.get(service);
        if (queue == null)
            return true;
        RowDataPacket rp = new RowDataPacket(fieldCount);
        rp.read(row);
        HeapItem item = new HeapItem(row, rp, (MySQLResponseService) service);
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            LOGGER.info("rowResponse error", e);
        }
        return false;
    }

    private void mergeFieldEof(List<byte[]> fields, MySQLResponseService service) throws IOException {
        fieldCount = fields.size();
        List<FieldPacket> fieldPackets = new ArrayList<>();
        for (byte[] field : fields) {
            FieldPacket fieldPacket = new FieldPacket();
            fieldPacket.read(field);
            if (rrs.getSchema() != null) {
                fieldPacket.setDb(rrs.getSchema().getBytes());
            }
            if (rrs.getTableAlias() != null) {
                fieldPacket.setTable(rrs.getTableAlias().getBytes());
            }
            if (rrs.getTable() != null) {
                fieldPacket.setOrgTable(rrs.getTable().getBytes());
            }
            fieldPackets.add(fieldPacket);
        }
        List<Order> orderBys = new ArrayList<>();
        for (String groupBy : rrs.getGroupByCols()) {
            ItemField itemField = new ItemField(rrs.getSchema(), rrs.getTableAlias(), groupBy);
            orderBys.add(new Order(itemField));
        }
        rowComparator = new RowDataComparator(HandlerTool.createFields(fieldPackets), orderBys);
        outputHandler.fieldEofResponse(null, null, fieldPackets, null, false, service);
    }

    private void startOwnThread() {
        DbleServer.getInstance().getComplexQueryExecutor().execute(() -> ownThreadJob());
    }

    private void ownThreadJob() {
        try {
            ArrayMinHeap<HeapItem> heap = new ArrayMinHeap<>((o1, o2) -> {
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
            });
            // init heap
            for (Map.Entry<MySQLResponseService, BlockingQueue<HeapItem>> entry : queues.entrySet()) {
                HeapItem firstItem = entry.getValue().take();
                heap.add(firstItem);
            }
            while (!heap.isEmpty()) {
                if (isFail())
                    return;
                HeapItem top = heap.peak();
                if (top.isNullItem()) {
                    heap.poll();
                } else {
                    BlockingQueue<HeapItem> topItemQueue = queues.get(top.getIndex());
                    HeapItem item = topItemQueue.take();
                    heap.replaceTop(item);
                    //limit
                    this.selectRows++;
                    if (rrs.getLimitSize() >= 0) {
                        if (selectRows <= rrs.getLimitStart()) {
                            continue;
                        } else if (selectRows > (rrs.getLimitStart() < 0 ? 0 : rrs.getLimitStart()) + rrs.getLimitSize()) {
                            noNeedRows = true;
                            while (!heap.isEmpty()) {
                                HeapItem itemToDiscard = heap.poll();
                                if (!itemToDiscard.isNullItem()) {
                                    BlockingQueue<HeapItem> discardQueue = queues.get(itemToDiscard.getIndex());
                                    while (true) {
                                        if (discardQueue.take().isNullItem() || isFail()) {
                                            break;
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                    }
                    outputHandler.rowResponse(top.getRowData(), top.getRowPacket(), false, top.getIndex());
                }
            }
            Iterator<Map.Entry<MySQLResponseService, BlockingQueue<HeapItem>>> iterator = this.queues.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<MySQLResponseService, BlockingQueue<HeapItem>> entry = iterator.next();
                entry.getValue().clear();
                session.releaseConnectionIfSafe(entry.getKey(), false);
                iterator.remove();
            }
            doSqlStat();
            outputHandler.rowEofResponse(null, false, null);
        } catch (Exception e) {
            String msg = "Merge thread error, " + e.getLocalizedMessage();
            LOGGER.info(msg, e);
            session.onQueryError(msg.getBytes());
        }
    }
}
