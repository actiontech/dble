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
 * From server to client in response to command, if no error and no result set.
 * <p>
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0
 * 1-9 (Length Coded Binary)   affected_rows
 * 1-9 (Length Coded Binary)   insert_id
 * 2                           server_status
 * 2                           warning_count
 * n   (until end of packet)   message fix:(Length Coded String)
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#OK_Packet
 * </pre>
 *
 * @author mycat
 */
public class OkPacket extends MySQLPacket {
    public static final byte FIELD_COUNT = 0x00;
    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};


    private byte fieldCount = FIELD_COUNT;
    private long affectedRows;
    private long insertId;
    private int serverStatus;
    private int warningCount;
    private byte[] message;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.getData());
        fieldCount = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();
        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    public OkPacket read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();
        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
        return this;
    }

    public ByteBuffer write(ByteBuffer buffer, AbstractConnection c) {

        int size = calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size,
                true);
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeLength(buffer, affectedRows);
        BufferUtil.writeLength(buffer, insertId);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, warningCount);
        if (message != null) {
            BufferUtil.writeWithLength(buffer, message);
        }

        return buffer;
    }

    public void write(ByteBuffer buffer, AbstractService service) {
        service.writeWithBuffer(this, buffer);
    }

    @Override
    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = write(c.allocate(), c);
        c.write(buffer);
    }


    public void markMoreResultsExists() {
        serverStatus = serverStatus | StatusFlags.SERVER_MORE_RESULTS_EXISTS;
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += BufferUtil.getLength(affectedRows);
        i += BufferUtil.getLength(insertId);
        i += 4;
        if (message != null) {
            i += BufferUtil.getLength(message);
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }

    public byte[] toBytes() {
        int size = calcPacketSize();
        ByteBuffer buffer = BufferPoolManager.getBufferPool().allocate(size + PACKET_HEADER_SIZE);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeLength(buffer, affectedRows);
        BufferUtil.writeLength(buffer, insertId);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, warningCount);
        if (message != null) {
            BufferUtil.writeWithLength(buffer, message);
        }
        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        BufferPoolManager.getBufferPool().recycle(buffer);
        return data;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    public long getInsertId() {
        return insertId;
    }

    public void setInsertId(long insertId) {
        this.insertId = insertId;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(int serverStatus) {
        this.serverStatus = serverStatus;
    }

    public int getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
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
}
