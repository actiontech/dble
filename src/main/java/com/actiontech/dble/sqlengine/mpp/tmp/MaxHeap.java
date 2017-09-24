/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp.tmp;

import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.ArrayList;
import java.util.List;

/**
 * MaxHeap FOR ASC
 *
 * @author coderczp-2014-12-8
 */
public class MaxHeap implements HeapItf {

    private RowDataCmp cmp;
    private List<RowDataPacket> data;

    public MaxHeap(RowDataCmp cmp, int size) {
        this.cmp = cmp;
        this.data = new ArrayList<>();
    }

    @Override
    public void buildHeap() {
        int len = data.size();
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapifyRecursive(i, len);
        }
    }

    private void heapify(int i, int size) {
        int max = 0;
        int mid = size >> 1; // ==size/2
        while (i <= mid) {
            max = i;
            int left = i << 1;
            int right = left + 1;
            if (left < size && cmp.compare(data.get(left), data.get(i)) > 0) {
                max = left;
            }
            if (right < size && cmp.compare(data.get(right), data.get(max)) > 0) {
                max = right;
            }
            if (i == max) {
                break;
            }
            if (i != max) {
                RowDataPacket tmp = data.get(i);
                data.set(i, data.get(max));
                data.set(max, tmp);
                i = max;
            }
        }

    }

    protected void heapifyRecursive(int i, int size) {
        int l = left(i);
        int r = right(i);
        int max = i;
        if (l < size && cmp.compare(data.get(l), data.get(i)) > 0) {
            max = l;
        }
        if (r < size && cmp.compare(data.get(r), data.get(max)) > 0) {
            max = r;
        }
        if (i == max) {
            return;
        }
        swap(i, max);
        heapifyRecursive(max, size);
    }


    private int right(int i) {
        return (i + 1) << 1;
    }

    private int left(int i) {
        return ((i + 1) << 1) - 1;
    }

    private void swap(int i, int j) {
        RowDataPacket tmp = data.get(i);
        RowDataPacket elementAt = data.get(j);
        data.set(i, elementAt);
        data.set(j, tmp);
    }

    @Override
    public RowDataPacket getRoot() {
        return data.get(0);
    }

    @Override
    public void setRoot(RowDataPacket root) {
        data.set(0, root);
        heapifyRecursive(0, data.size());
    }

    @Override
    public List<RowDataPacket> getData() {
        return data;
    }

    @Override
    public void add(RowDataPacket row) {
        data.add(row);
    }

    @Override
    public boolean addIfRequired(RowDataPacket row) {
        RowDataPacket root = getRoot();
        // remove the smallest
        if (cmp.compare(row, root) < 0) {
            setRoot(row);
            return true;
        }
        return false;
    }

    @Override
    public void heapSort(int size) {
        final int total = data.size();
        if (size <= 0 || size > total) {
            size = total;
        }
        final int min = size == total ? 0 : (total - size - 1);

        // change the tail and head
        for (int i = total - 1; i > min; i--) {
            swap(0, i);
            heapifyRecursive(0, i);
        }
    }

}
