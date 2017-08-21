package io.mycat.backend.mysql.store.diskbuffer;

import io.mycat.backend.mysql.nio.handler.util.RBTMinHeap;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.RowDataPacket;

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
    public DistinctResultDiskBuffer(BufferPool pool, int columnCount, RowDataComparator cmp) {
        super(pool, columnCount, cmp);
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
            this.heap = new RBTMinHeap<TapeItem>(this.heapCmp);
        heap.clear();
        for (ResultDiskTape tape : tapes) {
            addToHeap(tape);
        }
    }
}
