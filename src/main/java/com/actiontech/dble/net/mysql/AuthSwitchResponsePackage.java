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
 * Authentication Method Switch Response Packet.
 *
 * <p>
 * <pre>
 * Bytes          Name
 * -----          ----
 * string[EOF]    auth plugin response
 * </pre>
 * </p>
 *
 * @author collapsar
 */
public class AuthSwitchResponsePackage extends MySQLPacket {

    private byte[] authPluginData;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        authPluginData = mm.readBytes();
    }

    public byte[] getAuthPluginData() {
        return authPluginData;
    }

    @Override
    public void bufferWrite(AbstractConnection connection) {
        ByteBuffer buffer = connection.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(authPluginData);
        connection.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return authPluginData == null ? 0 : authPluginData.length;
    }

    @Override
    protected String getPacketInfo() {
        return "Authentication Method Switch Response Packet";
    }

    public void setAuthPluginData(byte[] authPluginData) {
        this.authPluginData = authPluginData;
    }
}
