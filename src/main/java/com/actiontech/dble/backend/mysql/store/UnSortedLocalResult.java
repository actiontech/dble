/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.backend.mysql.store.diskbuffer.UnSortedResultDiskBuffer;
import com.actiontech.dble.backend.mysql.store.result.ResultExternal;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.buffer.BufferPoolRecord;

public class UnSortedLocalResult extends LocalResult {

    public UnSortedLocalResult(int fieldsCount, BufferPool pool, String charset, BufferPoolRecord.Builder recordBuilder) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, charset, recordBuilder);
    }

    public UnSortedLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, String charset, BufferPoolRecord.Builder recordBuilder) {
        super(initialCapacity, fieldsCount, pool, charset, recordBuilder);
    }

    @Override
    protected ResultExternal makeExternal() {
        if (maxReadMemorySize != -1) {
            return new UnSortedResultDiskBuffer(pool, fieldsCount, maxReadMemorySize, recordBuilder);
        } else {
            return new UnSortedResultDiskBuffer(pool, fieldsCount, recordBuilder);
        }
    }

    @Override
    protected void beforeFlushRows() {

    }

    @Override
    protected void doneOnlyMemory() {

    }

}
