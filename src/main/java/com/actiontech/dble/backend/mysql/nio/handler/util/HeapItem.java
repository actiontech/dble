/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;


import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

public class HeapItem {

    private static final HeapItem NULL_ITEM;

    private byte[] row;
    private RowDataPacket rowPacket;
    private MySQLResponseService hashIndex;
    private boolean isNull = false;

    static {
        NULL_ITEM = new HeapItem();
        NULL_ITEM.isNull = true;
    }

    public static HeapItem nullItem() {
        return NULL_ITEM;
    }

    public boolean isNullItem() {
        return row == null && isNull;
    }

    private HeapItem() {
    }

    public HeapItem(byte[] row, RowDataPacket rdp, MySQLResponseService index) {
        this.row = row;
        this.rowPacket = rdp;
        this.hashIndex = index;
    }

    public MySQLResponseService getIndex() {
        return hashIndex;
    }

    public byte[] getRowData() {
        return row;
    }

    public RowDataPacket getRowPacket() {
        return this.rowPacket;
    }

}
