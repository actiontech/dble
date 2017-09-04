package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.backend.mysql.store.diskbuffer.UnSortedResultDiskBuffer;
import com.actiontech.dble.backend.mysql.store.result.ResultExternal;
import com.actiontech.dble.buffer.BufferPool;

public class UnSortedLocalResult extends LocalResult {

    public UnSortedLocalResult(int fieldsCount, BufferPool pool, String charset) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, charset);
    }

    public UnSortedLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, String charset) {
        super(initialCapacity, fieldsCount, pool, charset);
    }

    @Override
    protected ResultExternal makeExternal() {
        return new UnSortedResultDiskBuffer(pool, fieldsCount);
    }

    @Override
    protected void beforeFlushRows() {

    }

    @Override
    protected void doneOnlyMemory() {

    }

}
