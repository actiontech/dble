/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.StreamUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.apache.commons.lang.NotImplementedException;
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
 *
 * @see https://dev.mysql.com/doc/dev/mysql-server/9.1.0/page_protocol_connection_phase_packets_protocol_ssl_request.html
 * </pre>
 *
 * @author mycat
 */
public class SSLRequestPacket extends MySQLPacket {
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLRequestPacket.class);
    private static final byte[] FILLER = new byte[23];

    private long clientFlags;
    private long maxPacketSize;
    private int charsetIndex;

    private byte[] extra; // from FILLER(23)
    private String tenant = "";
    private boolean multStatementAllow = false;

    private boolean isSSLRequest = false;


    public void read(byte[] data) {
        throw new NotImplementedException();
    }

    public void write(OutputStream out) throws IOException {
        //        StreamUtil.writeUB3(out, calcPacketSize() - 1); //todo：？存疑
        StreamUtil.writeUB3(out, calcPacketSize());
        StreamUtil.write(out, packetId);
        StreamUtil.writeUB4(out, clientFlags);       // capability flags
        StreamUtil.writeUB4(out, maxPacketSize);
        StreamUtil.write(out, (byte) charsetIndex);
        out.write(FILLER);
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

        if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            //if use the mysql_native_password  is used for auth this need be replay
            BufferUtil.writeWithNull(buffer, HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN);
        }

        service.writeDirectly(buffer, getLastWriteFlag());
    }


    public void bufferWrite(OutputStream out) throws IOException {
        //        if (database != null) {
        StreamUtil.writeUB3(out, calcPacketSizeWithKey()); //todo：？存疑
        //        } else {
        //            StreamUtil.writeUB3(out, calcPacketSizeWithKey() - 1);
        //        }
        StreamUtil.write(out, packetId);
        StreamUtil.writeUB4(out, clientFlags);
        StreamUtil.writeUB4(out, maxPacketSize);
        StreamUtil.write(out, (byte) charsetIndex);
        out.write(FILLER);

    }

    @Override
    public void bufferWrite(AbstractConnection c) {

        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSizeWithKey());
        buffer.put(packetId);
        BufferUtil.writeUB4(buffer, clientFlags);     // capability flags
        BufferUtil.writeUB4(buffer, maxPacketSize);     // max-packet size
        buffer.put((byte) charsetIndex);                //character set
        buffer = c.getService().writeToBuffer(FILLER, buffer);       // reserved (all [0])


        c.getService().writeDirectly(buffer, getLastWriteFlag());

    }


    @Override
    public int calcPacketSize() {
        int size = 32; // 4+4+1+23;

        return size;
    }

    public int calcPacketSizeWithKey() {
        int size = 32; // 4+4+1+23;
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


    public String getTenant() {
        return tenant;
    }


    public boolean isMultStatementAllow() {
        return multStatementAllow;
    }

    public boolean getIsSSLRequest() {
        return isSSLRequest;
    }

    public boolean checkSSLRequest(MySQLMessage mm) {
        if (mm.position() == mm.length() && (clientFlags & Capabilities.CLIENT_SSL) != 0) {
            isSSLRequest = true;
            return true;
        } else {
            return false;
        }
    }


    @Override
    public boolean isEndOfQuery() {
        return true;
    }
}
