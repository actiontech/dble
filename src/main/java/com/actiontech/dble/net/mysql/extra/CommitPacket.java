/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.mysql.extra;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.CommandPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.singleton.BufferPoolManager;

import java.nio.ByteBuffer;

public class CommitPacket extends CommandPacket {

    public CommitPacket(byte command, byte[] arg, int packetId) {
        this.command = command;
        this.arg = arg;
        this.packetId = (byte) packetId;
    }

    public static CommitPacket initCommit() {
        return new CommitPacket(MySQLPacket.COM_QUERY, "commit".getBytes(), 0);
    }

    public byte[] toBytes() {
        int size = calcPacketSize();
        ByteBuffer buffer = BufferPoolManager.getBufferPool().allocate(size + PACKET_HEADER_SIZE);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(command);
        if (arg != null) {
            buffer.put(arg, 0, arg.length);
        }
        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        BufferPoolManager.getBufferPool().recycle(buffer);
        return data;
    }

    @Override
    public void bufferWrite(AbstractConnection connection) {

    }
}
