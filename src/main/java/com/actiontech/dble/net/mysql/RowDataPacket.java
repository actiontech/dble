/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.FrontendConnection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * From server to client. One packet for each row in the result set.
 * <p>
 * <pre>
 * Bytes                   Name
 * -----                   ----
 * n (Length Coded String) (column value)
 * ...
 *
 * (column value):         The data in the column, as a character string.
 *                         If a column is defined as non-character, the
 *                         server converts the value into a character
 *                         before sending it. Since the value is a Length
 *                         Coded String, a NULL can be represented with a
 *                         single byte containing 251(see the description
 *                         of Length Coded Strings in section "Elements" above).
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
 * </pre>
 *
 * @author mycat
 */
public class RowDataPacket extends MySQLPacket {
    protected static final byte NULL_MARK = (byte) 251;
    protected static final byte EMPTY_MARK = (byte) 0;

    private int fieldCount;
    public final List<byte[]> fieldValues;
    private List<byte[]> cmpValue;

    public RowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<>(fieldCount);
    }

    public void add(byte[] value) {
        fieldValues.add(value);
    }

    public void addFieldCount(int add) {
        fieldCount = fieldCount + add;
    }

    public void addAll(List<byte[]> values) {
        fieldValues.addAll(values);
    }

    public byte[] getValue(int index) {
        return fieldValues.get(index);
    }

    public void setValue(int index, byte[] value) {
        fieldValues.set(index, value);
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        for (int i = 0; i < fieldCount; i++) {
            fieldValues.add(mm.readBytesWithLength());
        }
    }

    @Override
    public ByteBuffer write(ByteBuffer bb, FrontendConnection c,
                            boolean writeSocketIfFull) {
        bb = c.checkWriteBuffer(bb, PACKET_HEADER_SIZE, writeSocketIfFull);
        BufferUtil.writeUB3(bb, calcPacketSize());
        bb.put(packetId);
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);
            if (fv == null) {
                bb = c.checkWriteBuffer(bb, 1, writeSocketIfFull);
                bb.put(RowDataPacket.NULL_MARK);
            } else if (fv.length == 0) {
                bb = c.checkWriteBuffer(bb, 1, writeSocketIfFull);
                bb.put(RowDataPacket.EMPTY_MARK);
            } else {
                bb = c.checkWriteBuffer(bb, BufferUtil.getLength(fv),
                        writeSocketIfFull);
                BufferUtil.writeLength(bb, fv.length);
                bb = c.writeToBuffer(fv, bb);
            }
        }
        return bb;
    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte[] v = fieldValues.get(i);
            size += (v == null || v.length == 0) ? 1 : BufferUtil.getLength(v);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL RowData Packet";
    }

    public byte[] toBytes() {
        int size = calcPacketSize();
        ByteBuffer buffer = DbleServer.getInstance().getBufferPool().allocate(size + PACKET_HEADER_SIZE);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);
            if (fv == null) {
                buffer.put(RowDataPacket.NULL_MARK);
            } else if (fv.length == 0) {
                buffer.put(RowDataPacket.EMPTY_MARK);
            } else {
                BufferUtil.writeWithLength(buffer, fv);
            }
        }
        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        DbleServer.getInstance().getBufferPool().recycle(buffer);
        return data;
    }


    public List<byte[]> getCmpValue() {
        return cmpValue;
    }

    public void setCmpValue(List<byte[]> cmpValue) {
        this.cmpValue = cmpValue;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }
}
