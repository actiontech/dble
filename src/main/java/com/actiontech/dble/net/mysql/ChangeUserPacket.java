/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.connection.AbstractConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ChangeUserPacket extends MySQLPacket {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeUserPacket.class);

    public static final byte STATUS = (byte) 0x11;

    private long clientFlags;
    private int charsetIndex;
    private String user;
    private byte[] password;
    private String database;
    private String authPlugin = "mysql_native_password";
    private String tenant = "";

    public ChangeUserPacket(long clientFlags, int charsetIndex) {
        this.clientFlags = clientFlags;
        this.charsetIndex = charsetIndex;
    }

    public ChangeUserPacket(String user) {
        this.user = user;
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        mm.position(5);
        user = mm.readStringWithNull(); //user name end by a [00]
        // it must be Capabilities.CLIENT_SECURE_CONNECTION
        password = mm.readBytesWithLength();
        database = mm.readStringWithNull();
        if (database != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            database = database.toLowerCase();
        }
        if (mm.hasRemaining()) {
            charsetIndex = mm.readUB2();
            if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
                authPlugin = mm.readStringWithNull();
            }
            if ((clientFlags & Capabilities.CLIENT_CONNECT_ATTRS) != 0) {
                long attrLength = mm.readLength();
                while (attrLength > 0) {
                    long start = mm.position();
                    String charsetName = CharsetUtil.getJavaCharset(charsetIndex);
                    try {
                        String key = mm.readStringWithLength(charsetName);
                        String value = mm.readStringWithLength(charsetName);
                        if (key.equals("tenant")) {
                            tenant = value;
                        }
                    } catch (Exception e) {
                        LOGGER.warn("read attribute filed", e);
                    }
                    attrLength -= (mm.position() - start);
                }
            }
        }
    }

    @Override
    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(STATUS);
        if (user != null) {
            byte[] userData = user.getBytes();
            buffer = c.checkWriteBuffer(buffer, userData.length + 1, true);
            BufferUtil.writeWithNull(buffer, userData);
        } else {
            buffer = c.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0x0);
        }
        if (password != null) {
            buffer = c.checkWriteBuffer(buffer, BufferUtil.getLength(password), true);
            BufferUtil.writeWithLength(buffer, password);
        } else {
            buffer = c.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0x0);
        }
        if (database != null) {
            byte[] databaseData = database.getBytes();
            buffer = c.checkWriteBuffer(buffer, databaseData.length + 1, true);
            BufferUtil.writeWithNull(buffer, database.getBytes());
        } else {
            buffer = c.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0x0);
        }
        BufferUtil.writeUB2(buffer, charsetIndex);
        //        if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
        buffer = c.checkWriteBuffer(buffer, authPlugin.getBytes().length + 1, true);
        BufferUtil.writeWithNull(buffer, authPlugin.getBytes());
        //        }
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        size += 2;
        size += (authPlugin == null) ? 1 : authPlugin.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL ChangeUser Packet";
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    public String getUser() {
        return user;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public byte[] getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public String getTenant() {
        return tenant;
    }

}
