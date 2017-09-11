/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.FrontendConnection;

import java.nio.ByteBuffer;

/**
 * From server to client during initial handshake.
 * <p>
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version
 * n (Null-Terminated String)   server_version
 * 4                            thread_id
 * 8                            scramble_buff
 * 1                            (filler) always 0x00
 * 2                            server_capabilities
 * 1                            server_language
 * 2                            server_status
 * 13                           (filler) always 0x00 ...
 * 13                           rest of scramble_buff (4.1)
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
 * </pre>
 *
 * @author mycat
 */
public class HandshakePacket extends MySQLPacket {
    private static final byte[] FILLER_13 = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    private byte protocolVersion;
    private byte[] serverVersion;
    private long threadId;
    private byte[] seed;
    private int serverCapabilities;
    private byte serverCharsetIndex;
    private int serverStatus;
    private byte[] restOfScrambleBuff;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.getData());
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2();
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();
        mm.move(13);
        restOfScrambleBuff = mm.readBytesWithNull();
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2();
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();
        mm.move(13);
        restOfScrambleBuff = mm.readBytesWithNull();
    }

    public void write(FrontendConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion);
        BufferUtil.writeUB4(buffer, threadId);
        BufferUtil.writeWithNull(buffer, seed);
        BufferUtil.writeUB2(buffer, serverCapabilities);
        buffer.put(serverCharsetIndex);
        BufferUtil.writeUB2(buffer, serverStatus);
        buffer.put(FILLER_13);
        //        buffer.position(buffer.position() + 13);
        BufferUtil.writeWithNull(buffer, restOfScrambleBuff);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += serverVersion.length; // n
        size += 5; // 1+4
        size += seed.length; // 8
        size += 19; // 1+2+1+2+13
        size += restOfScrambleBuff.length; // 12
        size += 1; // 1
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Handshake Packet";
    }

    public byte getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(byte protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public byte[] getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(byte[] serverVersion) {
        this.serverVersion = serverVersion;
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public byte[] getSeed() {
        return seed;
    }

    public void setSeed(byte[] seed) {
        this.seed = seed;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public void setServerCapabilities(int serverCapabilities) {
        this.serverCapabilities = serverCapabilities;
    }

    public byte getServerCharsetIndex() {
        return serverCharsetIndex;
    }

    public void setServerCharsetIndex(byte serverCharsetIndex) {
        this.serverCharsetIndex = serverCharsetIndex;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(int serverStatus) {
        this.serverStatus = serverStatus;
    }

    public byte[] getRestOfScrambleBuff() {
        return restOfScrambleBuff;
    }

    public void setRestOfScrambleBuff(byte[] restOfScrambleBuff) {
        this.restOfScrambleBuff = restOfScrambleBuff;
    }
}
