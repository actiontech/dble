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

/**
 * From Server To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 * <p>
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#EOF_Packet
 * </pre>
 *
 * @author mycat
 */
public class EOFPacket extends MySQLPacket {
    public static final byte FIELD_COUNT = (byte) 0xfe;

    private byte fieldCount = FIELD_COUNT;
    private int warningCount;
    private int status = 2;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        warningCount = mm.readUB2();
        status = mm.readUB2();
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c, boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + size, writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, warningCount);
        BufferUtil.writeUB2(buffer, status);
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        return 5; // 1+2+2;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL EOF Packet";
    }

    public byte[] toBytes() {
        int size = calcPacketSize();
        ByteBuffer buffer = DbleServer.getInstance().getBufferPool().allocate(size + PACKET_HEADER_SIZE);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, warningCount);
        BufferUtil.writeUB2(buffer, status);
        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        DbleServer.getInstance().getBufferPool().recycle(buffer);
        return data;
    }

    public byte getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(byte fieldCount) {
        this.fieldCount = fieldCount;
    }

    public int getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
