/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;

import com.actiontech.dble.net.connection.AbstractConnection;

import java.nio.ByteBuffer;

/**
 * From client to server when the client do heartbeat between cluster.
 * <p>
 * <pre>
 * Bytes         Name
 * -----         ----
 * 1             command
 * n             id
 *
 * @author mycat
 */
public class HeartbeatPacket extends MySQLPacket {

    private byte command;
    private long id;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        id = mm.readLength();
    }

    @Override
    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(command);
        BufferUtil.writeLength(buffer, id);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return 1 + BufferUtil.getLength(id);
    }

    @Override
    protected String getPacketInfo() {
        return "Heartbeat Packet";
    }

    public byte getCommand() {
        return command;
    }

    public void setCommand(byte command) {
        this.command = command;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
