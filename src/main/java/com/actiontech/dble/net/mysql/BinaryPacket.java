/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.StreamUtil;

import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public class BinaryPacket extends MySQLPacket {

    private byte[] data;

    public byte[] getPublicKey() {
        return publicKey;
    }

    private byte[] publicKey;

    private static final int AUTH_PLUGIN_OFFSET = 1;

    private static final int AUTH_PLUGIN_LENGTH = 22;


    public void read(InputStream in) throws IOException {
        packetLength = StreamUtil.readUB3(in);
        packetId = StreamUtil.read(in);
        byte[] ab = new byte[packetLength];
        StreamUtil.read(in, ab, 0, ab.length);
        data = ab;
    }


    public byte[] readKey(InputStream in) throws IOException {
        packetLength = StreamUtil.readUB3(in);
        packetId = StreamUtil.read(in);
        byte[] ab = new byte[packetLength];
        publicKey = StreamUtil.readKey(in, ab, 0, ab.length);
        return publicKey;
    }

    public byte[] readKey(byte[] dataContainsKey) throws IOException {
        packetLength = StreamUtil.readBackInt(dataContainsKey, 0, 3);
        publicKey = StreamUtil.read(dataContainsKey, 4, packetLength);
        return publicKey;
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, AbstractService service, boolean writeSocketIfFull) {
        buffer = service.checkWriteBuffer(buffer, PACKET_HEADER_SIZE, writeSocketIfFull);
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer = service.writeToBuffer(data, buffer);
        return buffer;
    }

    @Override
    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = c.checkWriteBuffer(buffer, PACKET_HEADER_SIZE + calcPacketSize(), false);
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(data);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return data == null ? 0 : data.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Binary Packet";
    }

    public byte[] getData() {
        return data;
    }

    public String getAuthPluginName() throws Exception {
        return new String(StreamUtil.read(data, AUTH_PLUGIN_OFFSET, AUTH_PLUGIN_LENGTH)).trim();
    }

    public String getAuthPluginName(byte[] dataContainsPluginName) throws Exception {
        return new String(StreamUtil.read(dataContainsPluginName, AUTH_PLUGIN_OFFSET + 4, AUTH_PLUGIN_LENGTH)).trim();
    }


    public byte[] getAuthPluginData() throws Exception {
        return StreamUtil.read(data, AUTH_PLUGIN_LENGTH + 1, AUTH_PLUGIN_LENGTH - 1);
    }


    public byte[] getAuthPluginData(byte[] dataContainsPluginData) throws Exception {
        return StreamUtil.read(dataContainsPluginData, AUTH_PLUGIN_LENGTH + 1 + 4, AUTH_PLUGIN_LENGTH - 1);
    }
    public void setData(byte[] data) {
        this.data = data;
    }
}
