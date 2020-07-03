/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;

import com.actiontech.dble.singleton.BufferPoolManager;

import java.nio.ByteBuffer;

/**
 * From server to client in response to command, if error.
 * <p>
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errNo
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 *
 * @author mycat
 */
public class ErrorPacket extends MySQLPacket {
    public static final byte FIELD_COUNT = (byte) 0xff;
    private static final byte SQLSTATE_MARKER = (byte) '#';
    private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    private byte fieldCount = FIELD_COUNT;
    private int errNo;
    private byte mark = SQLSTATE_MARKER;
    private byte[] sqlState = DEFAULT_SQLSTATE;
    private byte[] message;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.getData());
        fieldCount = mm.read();
        errNo = mm.readUB2();
        if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
            mm.read();
            sqlState = mm.readBytes(5);
        }
        message = mm.readBytes();
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        errNo = mm.readUB2();
        if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
            mm.read();
            sqlState = mm.readBytes(5);
        }
        message = mm.readBytes();
    }

    public byte[] toBytes() {
        int size = calcPacketSize();
        ByteBuffer buffer = BufferPoolManager.getBufferPool().allocate(size + PACKET_HEADER_SIZE);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, errNo);
        buffer.put(mark);
        buffer.put(sqlState);
        if (message != null) {
            buffer.put(message);
        }
        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        BufferPoolManager.getBufferPool().recycle(buffer);
        return data;
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, AbstractService service,
                            boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = service.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size,
                writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, errNo);
        buffer.put(mark);
        buffer.put(sqlState);
        if (message != null) {
            buffer = service.writeToBuffer(message, buffer);
        }
        return buffer;
    }


    @Override
    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = this.write(buffer, (AbstractService) c.getService(), true);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 9; // 1 + 2 + 1 + 5
        if (message != null) {
            size += message.length;
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Error Packet";
    }

    public byte getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(byte fieldCount) {
        this.fieldCount = fieldCount;
    }

    public int getErrNo() {
        return errNo;
    }

    public void setErrNo(int errNo) {
        this.errNo = errNo;
    }

    public byte getMark() {
        return mark;
    }

    public void setMark(byte mark) {
        this.mark = mark;
    }

    public byte[] getSqlState() {
        return sqlState;
    }

    public void setSqlState(byte[] sqlState) {
        this.sqlState = sqlState;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public boolean isEndOfQuery() {
        return true;
    }

    public boolean isEndOfSession() {
        return true;
    }
}
