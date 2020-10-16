/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.connection.AbstractConnection;

import java.nio.ByteBuffer;

/**
 * Authentication Method Switch Request Packet.
 *
 * <p>
 * <pre>
 * Bytes          Name
 * -----          ----
 * 1              [fe]
 * string[NUL]    plugin name
 * string[EOF]    auth plugin data
 * </pre>
 * </p>
 *
 * @author collapsar
 */
public class AuthSwitchRequestPackage extends MySQLPacket {

    public static final byte STATUS = (byte) 0xfe;
    private byte[] authPluginName;
    private byte[] authPluginData;

    public AuthSwitchRequestPackage() {
    }

    public AuthSwitchRequestPackage(byte[] authPluginName, byte[] authPluginData) {
        this.authPluginName = authPluginName;
        this.authPluginData = authPluginData;
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        mm.position(5);
        authPluginName = mm.readBytesWithNull();
        authPluginData = mm.readBytesWithNull();
    }

    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(STATUS);
        BufferUtil.writeWithNull(buffer, authPluginName);
        BufferUtil.writeWithNull(buffer, authPluginData);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += (authPluginName == null) ? 1 : BufferUtil.getLength(authPluginName);
        size += (authPluginData == null) ? 1 : BufferUtil.getLength(authPluginData);
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "Authentication Method Switch Request Packet";
    }

    public byte[] getAuthPluginName() {
        return authPluginName;
    }

    public byte[] getAuthPluginData() {
        return authPluginData;
    }
}
