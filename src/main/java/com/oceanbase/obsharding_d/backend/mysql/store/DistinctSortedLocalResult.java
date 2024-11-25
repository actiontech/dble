/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.store;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.util.RBTreeList;

public class DistinctSortedLocalResult extends DistinctLocalResult {
    public DistinctSortedLocalResult(BufferPool pool, int fieldsCount, RowDataComparator distinctCmp, String charset, BufferPoolRecord.Builder bufferRecordBuilder) {
        super(pool, fieldsCount, distinctCmp, charset, bufferRecordBuilder);
    }

    /**
     * @return next row
     */
    @Override
    public RowDataPacket next() {
        lock.lock();
        try {
            if (this.isClosed)
                return null;
            if (++rowId < rowCount) {
                if (external != null) {
                    currentRow = external.next();
                } else {
                    currentRow = ((RBTreeList<RowDataPacket>) rows).inOrderOf(rowId);
                }
            } else {
                currentRow = null;
            }
            return currentRow;
        } finally {
            lock.unlock();
        }
    }
}
