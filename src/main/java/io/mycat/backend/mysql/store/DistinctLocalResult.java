package io.mycat.backend.mysql.store;

import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.diskbuffer.DistinctResultDiskBuffer;
import io.mycat.backend.mysql.store.result.ResultExternal;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.RBTreeList;

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
                               String charset) {
        super(initialCapacity, fieldsCount, pool, charset);
        this.distinctCmp = distinctCmp;
        this.rows = new RBTreeList<RowDataPacket>(initialCapacity, distinctCmp);
    }

    public DistinctLocalResult(BufferPool pool, int fieldsCount, RowDataComparator distinctCmp, String charset) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, distinctCmp, charset);
    }

    @Override
    protected ResultExternal makeExternal() {
        return new DistinctResultDiskBuffer(pool, fieldsCount, distinctCmp);
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
