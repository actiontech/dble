/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;


import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.CallBackHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.store.UnSortedLocalResult;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.exception.TempTableException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.meta.TempTable;
import com.actiontech.dble.singleton.BufferPoolManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * get the tmp table's result
 */
public class DelayTableHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayTableHandler.class);

    private final ReentrantLock lock;
    private final TempTable tempTable;

    private int maxPartSize = 2000;
    private int maxConnSize = 4;
    private int rowCount = 0;
    private CallBackHandler tempDoneCallBack;
    // Handler generated by tempHandler will release by itself
    private DMLResponseHandler createdHandler;

    private int sourceSelIndex = -1;
    private final Item sourceSel;
    private Field sourceField;
    private Set<String> valueSet;
    private String tableAlias;

    public DelayTableHandler(long id, Session session, Item sourceSel) {
        super(id, session);
        this.lock = new ReentrantLock();
        this.tempTable = new TempTable();
        this.maxPartSize = SystemConfig.getInstance().getNestLoopRowsSize();
        this.maxConnSize = SystemConfig.getInstance().getNestLoopConnSize();
        this.sourceSel = sourceSel;
        this.valueSet = new HashSet<>();
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            session.setHandlerStart(this);
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                tempTable.setFieldPackets(this.fieldPackets);
                String charSet = !service.isFakeClosed() ? service.getCharset().getResults() : session.getSource().getService().getCharset().getResults();
                tempTable.setCharset(charSet);
                tempTable.setRowsStore(new UnSortedLocalResult(fieldPackets.size(), BufferPoolManager.getBufferPool(),
                        CharsetUtil.getJavaCharset(charSet), generateRecordBuilder()).setMemSizeController(session.getOtherBufferMC()));
                List<Field> fields = HandlerTool.createFields(this.fieldPackets);
                sourceSelIndex = HandlerTool.findField(sourceSel, fields, 0);
                if (sourceSelIndex < 0)
                    throw new TempTableException("sourcesel [" + sourceSel.toString() + "] not found in fields");
                sourceField = fields.get(sourceSelIndex);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        lock.lock();
        try {
            if (terminate.get()) {
                return true;
            }
            if (++rowCount > maxPartSize * maxConnSize) {
                String msg = "Property [nestLoopRowsSize] is " + maxPartSize + ",property [nestLoopConnSize] is " + maxConnSize +
                        ",nest loop temp table too much rows, number of temp table rows more than product of nestLoopConnSize and nestLoopRowsSize is " + maxPartSize * maxConnSize;
                String errMessage = msg + " , conn info [" + service + "] !";
                LOGGER.info(errMessage);
                throw new TempTableException(msg);
            }
            RowDataPacket row = rowPacket;
            if (row == null) {
                row = new RowDataPacket(this.fieldPackets.size());
                row.read(rowNull);
            }
            tempTable.addRow(row);
            sourceField.setPtr(row.getValue(sourceSelIndex));
            valueSet.add(sourceField.valStr());
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        lock.lock();
        try {
            // callBack after terminated
            if (terminate.get()) {
                return;
            }
            tempTable.dataEof();
            // locked onTerminate, because terminated may sync with start
            tempDoneCallBack.call();
            session.setHandlerEnd(this);
        } catch (Exception e) {
            LOGGER.info("rowEof exception!", e);
            throw new TempTableException("rowEof exception!", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void onTerminate() {
        lock.lock();
        try {
            this.tempTable.close();
            this.valueSet.clear();
            if (createdHandler != null) {
                HandlerTool.terminateHandlerTree(createdHandler);
            }
        } finally {
            lock.unlock();
        }
    }

    public TempTable getTempTable() {
        return tempTable;
    }

    public DMLResponseHandler getCreatedHandler() {
        return createdHandler;
    }

    public void setCreatedHandler(DMLResponseHandler createdHandler) {
        this.createdHandler = createdHandler;
    }

    public Set<String> getValueSet() {
        return valueSet;
    }

    public int getMaxPartSize() {
        return maxPartSize;
    }

    public CallBackHandler getTempDoneCallBack() {
        return tempDoneCallBack;
    }

    public void setTempDoneCallBack(CallBackHandler tempDoneCallBack) {
        this.tempDoneCallBack = tempDoneCallBack;
    }

    @Override
    public HandlerType type() {
        return HandlerType.TEMPTABLE;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }
}
