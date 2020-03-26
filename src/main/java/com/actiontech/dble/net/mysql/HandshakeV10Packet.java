/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;


import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.FrontendConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * From mycat server to client during initial handshake.
 * <p>
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version (always 0x0a)
 * n (string[NULL])             server_version
 * 4                            thread_id
 * 8 (string[8])                auth-plugin-data-part-1
 * 1                            (filler) always 0x00
 * 2                            capability flags (lower 2 bytes)
 *   if more data in the packet:
 * 1                            character set
 * 2                            status flags
 * 2                            capability flags (upper 2 bytes)
 *   if capabilities & CLIENT_PLUGIN_AUTH {
 * 1                            length of auth-plugin-data
 *   } else {
 * 1                            0x00
 *   }
 * 10 (string[10])              reserved (all 0x00)
 *   if capabilities & CLIENT_SECURE_CONNECTION {
 * string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
 *   }
 *   if capabilities & CLIENT_PLUGIN_AUTH {
 * string[NUL]    auth-plugin name
 * }
 *
 * </pre>
 *
 * @author CrazyPig
 * @since 2016-11-13
 */
public class HandshakeV10Packet extends MySQLPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandshakeV10Packet.class);


    private static final byte[] FILLER_10 = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    public static final byte[] NATIVE_PASSWORD_PLUGIN = "mysql_native_password".getBytes();
    public static final byte[] CACHING_SHA2_PASSWORD_PLUGIN = "caching_sha2_password".getBytes();

    private byte protocolVersion;
    private byte[] serverVersion;
    private long threadId;
    private byte[] seed; // auth-plugin-data-part-1

    private int serverCapabilities;
    private byte serverCharsetIndex;
    private int serverStatus;
    private byte[] restOfScrambleBuff; // auth-plugin-data-part-2
    private byte[] authPluginName = NATIVE_PASSWORD_PLUGIN;

    public byte[] getAuthPluginName() {
        return authPluginName;
    }

    public byte[] getServerVersion() {
        return serverVersion;
    }

    public boolean write(FrontendConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion);
        BufferUtil.writeUB4(buffer, threadId);
        BufferUtil.writeWithNull(buffer, seed);
        BufferUtil.writeUB2(buffer, serverCapabilities); // capability flags (lower 2 bytes)
        buffer.put(serverCharsetIndex);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, (serverCapabilities >> 16)); // capability flags (upper 2 bytes)
        buffer.put((byte) (seed.length + 13));
        buffer.put(FILLER_10);
        buffer.put(restOfScrambleBuff);            //auth-plugin-data-part-2
        // restOfScrambleBuff.length always to be 12
        for (int i = 13 - restOfScrambleBuff.length; i > 0; i--) {
            buffer.put((byte) 0);
        }
        BufferUtil.writeWithNull(buffer, authPluginName);
        return c.registerWrite(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 1; // protocol version
        size += (serverVersion.length + 1); // server version
        size += 4; // connection id
        size += seed.length;
        size += 1; // [00] filler
        size += 2; // capability flags (lower 2 bytes)
        size += 1; // character set
        size += 2; // status flags
        size += 2; // capability flags (upper 2 bytes)
        size += 1;
        size += 10; // reserved (all [00])
        // restOfScrambleBuff.length always to be 12
        size += 13;
        size += (authPluginName.length + 1); // auth-plugin name
        return size;
    }


    /**
     * @param bin bin
     */
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
        //get the complete serverCapabilities
        serverCapabilities = serverCapabilities | (mm.readUB2() << 16);
        //length of auth-plugin-data for 1 byte
        int authPluginData = mm.read();
        mm.move(10);
        restOfScrambleBuff = mm.readBytesWithInputLength(authPluginData - 9 > 12 ? 12 : authPluginData - 9);
        mm.move(1);
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            authPluginName = mm.readBytesWithNull();
        }
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
        //get the complete serverCapabilities
        serverCapabilities = serverCapabilities | (mm.readUB2() << 16);
        //length of auth-plugin-data for 1 byte
        int authPluginData = mm.read();
        mm.move(10);
        restOfScrambleBuff = mm.readBytesWithInputLength(authPluginData - 9 > 12 ? 12 : authPluginData - 9);
        mm.move(1);
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            authPluginName = mm.readBytesWithNull();
        }
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL HandshakeV10 Packet";
    }

    public void setProtocolVersion(byte protocolVersion) {
        this.protocolVersion = protocolVersion;
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
