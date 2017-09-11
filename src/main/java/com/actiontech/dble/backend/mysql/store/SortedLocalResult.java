/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.backend.mysql.store.diskbuffer.SortedResultDiskBuffer;
import com.actiontech.dble.backend.mysql.store.result.ResultExternal;
import com.actiontech.dble.buffer.BufferPool;

import java.util.Collections;

public class SortedLocalResult extends LocalResult {

    protected RowDataComparator rowcmp;

    public SortedLocalResult(BufferPool pool, int fieldsCount, RowDataComparator rowcmp, String charset) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, rowcmp, charset);
    }

    public SortedLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, RowDataComparator rowcmp,
                             String charset) {
        super(initialCapacity, fieldsCount, pool, charset);
        this.rowcmp = rowcmp;
    }

    @Override
    protected ResultExternal makeExternal() {
        return new SortedResultDiskBuffer(pool, fieldsCount, rowcmp);
    }

    @Override
    protected void beforeFlushRows() {
        Collections.sort(rows, this.rowcmp);
    }

    @Override
    protected void doneOnlyMemory() {
        Collections.sort(rows, this.rowcmp);
    }

}
