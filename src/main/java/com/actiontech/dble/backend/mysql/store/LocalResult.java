/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.backend.mysql.store.memalloc.MemSizeController;
import com.actiontech.dble.backend.mysql.store.result.ResultExternal;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.external.ResultStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class LocalResult implements ResultStore {

    protected static final int DEFAULT_INITIAL_CAPACITY = 1024;
    protected final int fieldsCount;
    protected int maxMemory = 262144;

    protected BufferPool pool;
    protected List<RowDataPacket> rows;
    protected ResultExternal external;
    protected int rowId, rowCount;
    protected int currentMemory;
    protected RowDataPacket currentRow;
    protected RowDataPacket lastRow;
    protected boolean isClosed;
    protected Lock lock;
    /* @bug 1208 */
    protected String charset = "UTF-8";
    protected MemSizeController bufferMC;

    public LocalResult(int initialCapacity, int fieldsCount, BufferPool pool, String charset) {
        this.rows = new ArrayList<>(initialCapacity);
        this.fieldsCount = fieldsCount;
        this.pool = pool;
        init();
        this.isClosed = false;
        this.lock = new ReentrantLock();
        this.charset = charset;
    }

    /**
     * add a row into localresult
     *
     * @param row
     */
    public void add(RowDataPacket row) {
        lock.lock();
        try {
            if (this.isClosed)
                return;
            lastRow = row;
            rows.add(row);
            rowCount++;
            int increSize = getRowMemory(row);
            currentMemory += increSize;
            boolean needFlush = false;
            if (bufferMC != null) {
                if (!bufferMC.addSize(increSize)) {
                    needFlush = true;
                }
            } else if (!needFlush && currentMemory > maxMemory) {
                needFlush = true;
            }
            if (needFlush) {
                if (external == null)
                    external = makeExternal();
                addRowsToDisk();
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract ResultExternal makeExternal();

    public RowDataPacket currentRow() {
        return currentRow;
    }

    public RowDataPacket getLastRow() {
        return lastRow;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getRowId() {
        return rowId;
    }

    /**
     * @return next row
     */
    public RowDataPacket next() {
        lock.lock();
        try {
            if (this.isClosed)
                return null;
            if (++rowId < rowCount) {
                if (external != null) {
                    currentRow = external.next();
                } else {
                    currentRow = rows.get(rowId);
                }
            } else {
                currentRow = null;
            }
            return currentRow;
        } finally {
            lock.unlock();
        }
    }

    /**
     * This method is called after all rows have been added.
     */
    public void done() {
        lock.lock();
        try {
            if (this.isClosed)
                return;
            if (external == null)
                doneOnlyMemory();
            else {
                if (!rows.isEmpty())
                    addRowsToDisk();
                external.done();
            }
            reset();
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doneOnlyMemory();

    public void reset() {
        lock.lock();
        try {
            rowId = -1;
            if (external != null) {
                external.reset();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (this.isClosed)
                return;
            this.isClosed = true;
            rows.clear();
            if (bufferMC != null)
                bufferMC.subSize(currentMemory);
            rows = null;
            if (external != null) {
                external.close();
                external = null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            rows.clear();
            if (bufferMC != null)
                bufferMC.subSize(currentMemory);
            init();
            if (external != null) {
                external.close();
                external = null;
            }
        } finally {
            lock.unlock();
        }
    }

    protected final void addRowsToDisk() {
        beforeFlushRows();
        rowCount = external.addRows(rows);
        rows.clear();
        if (bufferMC != null)
            bufferMC.subSize(currentMemory);
        currentMemory = 0;
    }

    /**
     * job to do before flush rows into disk
     */
    protected abstract void beforeFlushRows();

    protected int getRowMemory(RowDataPacket row) {
        return row.calcPacketSize();
    }

    private void init() {
        this.rowId = -1;
        this.rowCount = 0;
        this.currentMemory = 0;
        this.currentRow = null;
        this.lastRow = null;
    }

    public LocalResult setMemSizeController(MemSizeController memSizeController) {
        this.bufferMC = memSizeController;
        return this;
    }
}
