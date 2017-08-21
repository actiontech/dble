package io.mycat.plan.common.meta;

import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.exception.TempTableException;
import io.mycat.plan.common.external.ResultStore;

import java.util.Date;
import java.util.List;

public class TempTable {
    private String createdtime;
    private List<FieldPacket> fieldPackets;
    private ResultStore rowsStore;
    private String charset;

    public TempTable() {
        this.createdtime = new Date().toString();
    }

    public void addRow(RowDataPacket row) {
        this.rowsStore.add(row);
    }

    public void dataEof() {
        this.rowsStore.done();
    }

    public RowDataPacket nextRow() {
        if (this.rowsStore == null)
            throw new TempTableException("exception happend when try to get temptable");
        return this.rowsStore.next();
    }

    public void close() {
        if (rowsStore != null)
            rowsStore.close();
    }

    public String getCreatedtime() {
        return createdtime;
    }

    public List<FieldPacket> getFieldPackets() {
        return fieldPackets;
    }

    public String getCharset() {
        return charset;
    }

    public ResultStore getRowsStore() {
        return rowsStore;
    }

    public void setRowsStore(ResultStore rowsStore) {
        this.rowsStore = rowsStore;
    }

    public void setFieldPackets(List<FieldPacket> fieldPackets) {
        this.fieldPackets = fieldPackets;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

}
