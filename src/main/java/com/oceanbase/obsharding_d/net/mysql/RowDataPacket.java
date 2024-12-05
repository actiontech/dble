/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.mysql;

import com.oceanbase.obsharding_d.backend.mysql.BufferUtil;
import com.oceanbase.obsharding_d.backend.mysql.ByteUtil;
import com.oceanbase.obsharding_d.backend.mysql.MySQLMessage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.RowDataComparator;
import com.oceanbase.obsharding_d.buffer.BufferPool;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.singleton.BufferPoolManager;
import com.oceanbase.obsharding_d.statistic.sql.StatisticListener;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private Map<RowDataComparator, List<byte[]>> cmpValues;

    public RowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<>(fieldCount);
        cmpValues = new HashMap<>(1);
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
    public ByteBuffer write(ByteBuffer bb, AbstractService service,
                            boolean writeSocketIfFull) {
        StatisticListener.getInstance().record(service, r -> r.onFrontendAddRows());
        int size = calcPacketSize();
        int totalSize = size + PACKET_HEADER_SIZE;
        boolean isBigPackage = size >= MySQLPacket.MAX_PACKET_SIZE;
        if (isBigPackage) {
            service.writeDirectly(bb, WriteFlags.PART);
            ByteBuffer tmpBuffer = service.allocate(totalSize);
            BufferUtil.writeUB3(tmpBuffer, calcPacketSize());
            tmpBuffer.put(packetId);
            writeBody(tmpBuffer);
            tmpBuffer.flip();
            byte[] array = new byte[tmpBuffer.limit()];
            tmpBuffer.get(array);
            service.recycleBuffer(tmpBuffer);
            ByteBuffer newBuffer = service.allocate();
            return service.writeToBuffer(array, newBuffer);
        } else {
            bb = service.checkWriteBuffer(bb, totalSize, writeSocketIfFull);
            BufferUtil.writeUB3(bb, calcPacketSize());
            bb.put(packetId);
            for (int i = 0; i < fieldCount; i++) {
                byte[] fv = fieldValues.get(i);
                if (fv == null) {
                    bb = service.checkWriteBuffer(bb, 1, writeSocketIfFull);
                    bb.put(RowDataPacket.NULL_MARK);
                } else if (fv.length == 0) {
                    bb = service.checkWriteBuffer(bb, 1, writeSocketIfFull);
                    bb.put(RowDataPacket.EMPTY_MARK);
                } else {
                    bb = service.checkWriteBuffer(bb, BufferUtil.getLength(fv),
                            writeSocketIfFull);
                    BufferUtil.writeLength(bb, fv.length);
                    bb = service.writeToBuffer(fv, bb);
                }
            }
            return bb;
        }
    }


    private void writeBody(ByteBuffer buffer) {
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
        int packageNum = size / MAX_PACKET_SIZE + 1;
        if (packageNum > 1) {
            BufferPool bufferPool = BufferPoolManager.getBufferPool();
            ByteBuffer tmpBuffer = bufferPool.allocate(size, null);
            writeBody(tmpBuffer);
            tmpBuffer.flip();
            byte[] tmpArray = new byte[tmpBuffer.limit()];
            tmpBuffer.get(tmpArray);
            bufferPool.recycle(tmpBuffer);
            byte[] packets = new byte[size + PACKET_HEADER_SIZE * packageNum];
            int length = size;
            int singlePacketTotalSize = MAX_PACKET_SIZE + PACKET_HEADER_SIZE;
            for (int i = 0; i < packageNum; i++) {
                int singlePacketSize = MAX_PACKET_SIZE;
                if (length < MAX_PACKET_SIZE) {
                    singlePacketSize = length;
                }
                ByteUtil.writeUB3(packets, singlePacketSize, i * singlePacketTotalSize);
                packets[i * singlePacketTotalSize + 3] = packetId++;
                System.arraycopy(tmpArray, i * MAX_PACKET_SIZE, packets, i * singlePacketTotalSize + 4, singlePacketSize);
                length -= MAX_PACKET_SIZE;
            }
            return packets;
        } else {
            ByteBuffer buffer = BufferPoolManager.getBufferPool().allocate(size + PACKET_HEADER_SIZE, null);
            BufferUtil.writeUB3(buffer, calcPacketSize());
            buffer.put(packetId);
            writeBody(buffer);
            buffer.flip();
            byte[] data = new byte[buffer.limit()];
            buffer.get(data);
            BufferPoolManager.getBufferPool().recycle(buffer);
            return data;
        }
    }


    public List<byte[]> getCmpValue(RowDataComparator comparator) {
        return cmpValues.get(comparator);
    }

    public void cacheCmpValue(RowDataComparator comparator, List<byte[]> cmpValue) {
        this.cmpValues.put(comparator, cmpValue);
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }


    public List<byte[]> getFieldValues() {
        return fieldValues;
    }

    @Override
    public boolean isEndOfQuery() {
        return false;
    }
}
