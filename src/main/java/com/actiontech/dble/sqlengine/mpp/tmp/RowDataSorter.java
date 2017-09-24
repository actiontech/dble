/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp.tmp;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.sqlengine.mpp.OrderCol;
import com.actiontech.dble.sqlengine.mpp.RowDataPacketSorter;

import java.util.Collections;
import java.util.List;

/**
 * @author coderczp-2014-12-8
 */
public class RowDataSorter extends RowDataPacketSorter {

    // total(=offset+limit)
    private volatile int total;
    // record size(=limit)
    private volatile int size;
    // heap
    private volatile HeapItf heap;
    // compartor
    private volatile RowDataCmp cmp;
    // has been buildHeap
    private volatile boolean hasBuild;

    public RowDataSorter(OrderCol[] orderCols) {
        super(orderCols);
        this.cmp = new RowDataCmp(orderCols);
    }

    public synchronized void setLimit(int start, int offset) {
        if (start < 0) {
            start = 0;
        }
        if (offset <= 0) {
            this.total = this.size = Integer.MAX_VALUE;
        } else {
            this.total = start + offset;
            this.size = offset;
        }
        // use ASC, comparer will know ASC/DESC
        this.heap = new MaxHeap(cmp, total);
    }

    @Override
    public synchronized boolean addRow(RowDataPacket row) {
        if (heap.getData().size() < total) {
            heap.add(row);
            return true;
        }
        if (heap.getData().size() == total && !hasBuild) {
            heap.buildHeap();
            hasBuild = true;
        }
        return heap.addIfRequired(row);
    }

    @Override
    public List<RowDataPacket> getSortedResult() {
        final List<RowDataPacket> data = heap.getData();
        if (data.size() < 2) {
            return data;
        }

        if (total - size > data.size()) {
            return Collections.emptyList();
        }

        if (!hasBuild) {
            heap.buildHeap();
        }
        heap.heapSort(this.size);
        return heap.getData();
    }

    public RowDataCmp getCmp() {
        return cmp;
    }

}
