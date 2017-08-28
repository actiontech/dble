package io.mycat.sqlengine.mpp;


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
