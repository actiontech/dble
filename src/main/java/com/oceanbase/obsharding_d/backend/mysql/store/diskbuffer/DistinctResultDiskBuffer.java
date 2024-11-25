/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.store.diskbuffer;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RBTMinHeap;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;

/**
 * disk result buffer which show the distinct row result
 *
 * @author ActionTech
 */
public class DistinctResultDiskBuffer extends SortedResultDiskBuffer {

    /**
     * @param pool
     * @param columnCount
     * @param cmp
     */
    public DistinctResultDiskBuffer(BufferPool pool, int columnCount, RowDataComparator cmp, BufferPoolRecord.Builder bufferRecordBuilder) {
        super(pool, columnCount, cmp, bufferRecordBuilder);
    }

    @Override
    public RowDataPacket next() {
        if (heap.isEmpty())
            return null;
        TapeItem tapeItem = heap.poll();
        addToHeap(tapeItem.tape);
        return tapeItem.row;
    }

    /**
     * if heap already contains row, no add into heap
     *
     * @param tape
     */
    protected void addToHeap(ResultDiskTape tape) {
        while (true) {
            RowDataPacket row = tape.nextRow();
            if (row == null)
                return;
            else {
                TapeItem tapeItem = new TapeItem(row, tape);
                TapeItem oldItem = heap.find(tapeItem);
                if (oldItem == null) {
                    heap.add(tapeItem);
                    return;
                } else {
                    onFoundRow(oldItem.row, row);
                }
            }
        }
    }

    protected void onFoundRow(RowDataPacket oldRow, RowDataPacket row) {

    }

    @Override
    protected void resetHeap() {
        if (heap == null)
            this.heap = new RBTMinHeap<>(this.heapCmp);
        heap.clear();
        for (ResultDiskTape tape : tapes) {
            addToHeap(tape);
        }
    }
}
