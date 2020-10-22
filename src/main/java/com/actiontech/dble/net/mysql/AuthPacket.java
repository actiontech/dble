/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.StreamUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


/**
 * From client to server during initial handshake.
 * <p>
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 *
 * @author mycat
 */
public class AuthPacket extends MySQLPacket {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthPacket.class);
    private static final byte[] FILLER = new byte[23];

    private long clientFlags;
    private long maxPacketSize;
    private int charsetIndex;

    private byte[] extra; // from FILLER(23)
    private String user;
    private byte[] password;
    private String database;
    private String authPlugin;
    private String tenant = "";
    private boolean multStatementAllow = false;

    public void setAuthPlugin(String authPlugin) {
        this.authPlugin = authPlugin;
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        clientFlags = mm.readUB4(); // capability flags
        maxPacketSize = mm.readUB4();   //maxoacketSize
        charsetIndex = (mm.read() & 0xff); // character set
        // read extra
        int current = mm.position();
        int len = (int) mm.readLength();
        if (len > 0 && len < FILLER.length) {    //reserved
            byte[] ab = new byte[len];
            System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
            this.extra = ab;
        }
        mm.position(current + FILLER.length);
        user = mm.readStringWithNull(); //user name end by a [00]
        password = mm.readBytesWithLength(); //CLIENT_SECURE_CONNECTION
        if ((clientFlags & Capabilities.CLIENT_MULTIPLE_STATEMENTS) != 0) {
            multStatementAllow = true;
        }
        boolean clientWithDbJdbcBug = false;
        if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0)) {
            database = mm.readStringWithNull();
            if (database != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                database = database.toLowerCase();
            }
        } else {
            clientWithDbJdbcBug = true;
        }

        if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            if (clientWithDbJdbcBug && mm.read(mm.position()) == 0) {
                mm.read();
                clientWithDbJdbcBug = false;
            }
            authPlugin = mm.readStringWithNull();
        }
        // parsing user:tenant or connection attributes
        String[] userSplit = user.split(":");
        if (userSplit.length == 1 && (clientFlags & Capabilities.CLIENT_CONNECT_ATTRS) != 0) {
            //use from connection attributes
            if (clientWithDbJdbcBug && mm.read(mm.position()) == 0) {
                mm.read();
            }
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
        } else if (userSplit.length == 2) {
            //use from user:tenant
            user = userSplit[0];
            tenant = userSplit[1];
        }
    }

    public void write(OutputStream out) throws IOException {
        if (database != null) {
            StreamUtil.writeUB3(out, calcPacketSize());
        } else {
            StreamUtil.writeUB3(out, calcPacketSize() - 1);
        }
        StreamUtil.write(out, packetId);
        StreamUtil.writeUB4(out, clientFlags);       // capability flags
        StreamUtil.writeUB4(out, maxPacketSize);
        StreamUtil.write(out, (byte) charsetIndex);
        out.write(FILLER);
        if (user == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithNull(out, user.getBytes());
        }
        if (password == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithLength(out, password);
        }
        if (database != null) {
            StreamUtil.writeWithNull(out, database.getBytes());
        }
    }

    @Override
    public void write(MySQLResponseService service) {
        ByteBuffer buffer = service.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        BufferUtil.writeUB4(buffer, clientFlags);       // capability flags
        BufferUtil.writeUB4(buffer, maxPacketSize);     // max-packet size
        buffer.put((byte) charsetIndex);                //character set
        buffer = service.writeToBuffer(FILLER, buffer);       // reserved (all [0])
        if (user == null) {
            buffer = service.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0);
        } else {
            byte[] userData = user.getBytes();
            buffer = service.checkWriteBuffer(buffer, userData.length + 1, true);
            BufferUtil.writeWithNull(buffer, userData);
        }
        if (password == null) {
            buffer = service.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0);
        } else {
            buffer = service.checkWriteBuffer(buffer, BufferUtil.getLength(password), true);
            BufferUtil.writeWithLength(buffer, password);
        }
        if (database == null) {
            buffer = service.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0);
        } else {
            byte[] databaseData = database.getBytes();
            buffer = service.checkWriteBuffer(buffer, databaseData.length + 1, true);
            BufferUtil.writeWithNull(buffer, databaseData);
        }
        if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            //if use the mysql_native_password  is used for auth this need be replay
            BufferUtil.writeWithNull(buffer, HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN);
        }

        service.writeDirectly(buffer);
    }


    public void bufferWrite(OutputStream out) throws IOException {
        if (database != null) {
            StreamUtil.writeUB3(out, calcPacketSizeWithKey());
        } else {
            StreamUtil.writeUB3(out, calcPacketSizeWithKey() - 1);
        }
        StreamUtil.write(out, packetId);
        StreamUtil.writeUB4(out, clientFlags);
        StreamUtil.writeUB4(out, maxPacketSize);
        StreamUtil.write(out, (byte) charsetIndex);
        out.write(FILLER);
        if (user == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithNull(out, user.getBytes());
        }
        if (password == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithLength(out, password);
        }
        if (database != null) {
            StreamUtil.writeWithNull(out, database.getBytes());
        }
        if (authPlugin != null) {
            StreamUtil.writeWithNull(out, authPlugin.getBytes());
        }
    }

    public void bufferWrite(AbstractConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSizeWithKey());
        buffer.put(packetId);
        BufferUtil.writeUB4(buffer, clientFlags);     // capability flags
        BufferUtil.writeUB4(buffer, maxPacketSize);     // max-packet size
        buffer.put((byte) charsetIndex);                //character set
        buffer = c.writeToBuffer(FILLER, buffer);       // reserved (all [0])
        if (user == null) {
            buffer = c.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0);
        } else {
            byte[] userData = user.getBytes();
            buffer = c.checkWriteBuffer(buffer, userData.length + 1, true);
            BufferUtil.writeWithNull(buffer, userData);
        }
        if (password == null) {
            buffer = c.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0);
        } else {
            buffer = c.checkWriteBuffer(buffer, BufferUtil.getLength(password), true);
            BufferUtil.writeWithLength(buffer, password);
        }
        if (database == null) {
            buffer = c.checkWriteBuffer(buffer, 1, true);
            buffer.put((byte) 0);
        } else {
            byte[] databaseData = database.getBytes();
            buffer = c.checkWriteBuffer(buffer, databaseData.length + 1, true);
            BufferUtil.writeWithNull(buffer, databaseData);
        }
        //if use the mysql_native_password  is used for auth this need be replay
        BufferUtil.writeWithNull(buffer, authPlugin.getBytes());

        c.write(buffer);
    }


    @Override
    public int calcPacketSize() {
        int size = 32; // 4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    public int calcPacketSizeWithKey() {
        int size = 32; // 4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        size += (authPlugin == null) ? 1 : authPlugin.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Authentication Packet";
    }

    public long getClientFlags() {
        return clientFlags;
    }

    public void setClientFlags(long clientFlags) {
        this.clientFlags = clientFlags;
    }

    public long getMaxPacketSize() {
        return maxPacketSize;
    }

    public void setMaxPacketSize(long maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    public byte[] getExtra() {
        return extra;
    }

    public String getUser() {
        return user;
    }

    public String getTenant() {
        return tenant;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public boolean isMultStatementAllow() {
        return multStatementAllow;
    }
}
