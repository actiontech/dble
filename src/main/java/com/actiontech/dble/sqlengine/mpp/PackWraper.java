/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;


/**
 * Created by zagnix on 2016/7/6.
 */

/**
 * PackWraper konws its datanode.
 */
public final class PackWraper {
    private byte[] rowData;
    private String dataNode;

    public byte[] getRowData() {
        return rowData;
    }

    public void setRowData(byte[] rowData) {
        this.rowData = rowData;
    }

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }
}
