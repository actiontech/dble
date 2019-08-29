/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.Capabilities;

public class ChangeUserPacket extends MySQLPacket {
    private final long clientFlags;
    private int charsetIndex;
    private String user;
    private byte[] password;
    private String database;
    private String authPlugin = "mysql_native_password";

    public ChangeUserPacket(long clientFlags, int charsetIndex) {
        this.clientFlags = clientFlags;
        this.charsetIndex = charsetIndex;
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
        }
    }

    @Override
    public int calcPacketSize() {
        return packetLength;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL ChangeUser Packet";
    }

    public int getCharsetIndex() {
        return charsetIndex;
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

}
