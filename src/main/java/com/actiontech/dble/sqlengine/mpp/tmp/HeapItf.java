/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp.tmp;

import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.List;

/**
 * @author coderczp-2014-12-17
 */
public interface HeapItf {

    /**
     * buildHeap
     */
    void buildHeap();

    /**
     * getRoot
     *
     * @return
     */
    RowDataPacket getRoot();

    /**
     * ADD ITEM
     *
     * @param row
     */
    void add(RowDataPacket row);

    /**
     * getData
     *
     * @return
     */
    List<RowDataPacket> getData();

    /**
     * setRoot
     *
     * @param root
     */
    void setRoot(RowDataPacket root);

    /**
     * addIfRequired
     *
     * @param row
     */
    boolean addIfRequired(RowDataPacket row);

    /**
     * heapSort
     */
    void heapSort(int size);

}
