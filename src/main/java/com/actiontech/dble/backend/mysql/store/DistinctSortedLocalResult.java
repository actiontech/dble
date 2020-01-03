/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.RBTreeList;

public class DistinctSortedLocalResult extends DistinctLocalResult {
    public DistinctSortedLocalResult(BufferPool pool, int fieldsCount, RowDataComparator distinctCmp, String charset) {
        super(pool, fieldsCount, distinctCmp, charset);
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
