/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.Iterator;

/**
 * @author dcy
 * Create Date: 2020-12-31
 */
public interface CursorCache {
    String CHARSET = "UTF-8";


    void add(RowDataPacket row);


    void done();

    boolean isDone();

    /**
     * using Iterator to reduce resource usage
     *
     * @param expectRowNum
     * @return
     */
    Iterator<RowDataPacket> fetchBatch(long expectRowNum);


    int getRowCount();


    void close();
}
