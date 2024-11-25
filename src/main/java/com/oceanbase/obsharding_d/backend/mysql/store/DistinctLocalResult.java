/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.store;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.backend.mysql.store.diskbuffer.DistinctResultDiskBuffer;
import com.oceanbase.obsharding_d.backend.mysql.store.result.ResultExternal;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.util.RBTreeList;

/**
 * localresult to distinct input rows
 *
 * @author ActionTech
 */
public class DistinctLocalResult extends LocalResult {

    private RowDataComparator distinctCmp;

    /**
     * @param initialCapacity
     * @param fieldsCount
     * @param pool
     * @param distinctCmp
     * @param charset         distinct selectable compator
     */
    public DistinctLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, RowDataComparator distinctCmp,
                               String charset, BufferPoolRecord.Builder bufferRecordBuilder) {
        super(initialCapacity, fieldsCount, pool, charset, bufferRecordBuilder);
        this.distinctCmp = distinctCmp;
        this.rows = new RBTreeList<>(initialCapacity, distinctCmp);
    }

    public DistinctLocalResult(BufferPool pool, int fieldsCount, RowDataComparator distinctCmp, String charset, BufferPoolRecord.Builder bufferRecordBuilder) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, distinctCmp, charset, bufferRecordBuilder);
    }

    @Override
    protected ResultExternal makeExternal() {
        return new DistinctResultDiskBuffer(pool, fieldsCount, distinctCmp, bufferRecordBuilder);
    }

    /**
     * add a row into distinct localresult,if rows.contains(row),do not add
     *
     * @param row
     */
    @Override
    public void add(RowDataPacket row) {
        lock.lock();
        try {
            if (isClosed)
                return;
            int index = rows.indexOf(row);
            if (index >= 0)
                return;
            super.add(row);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doneOnlyMemory() {
        // Collections.sort(rows, this.distinctCmp);
    }

    @Override
    protected void beforeFlushRows() {
        // rbtree.toarray() is sorted,so do not need to sort again
    }

}
