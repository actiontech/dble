package io.mycat.backend.mysql.store;

import io.mycat.backend.mysql.store.diskbuffer.UnSortedResultDiskBuffer;
import io.mycat.backend.mysql.store.result.ResultExternal;
import io.mycat.buffer.BufferPool;

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
