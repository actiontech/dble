/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.heartbeat.DBHeartbeat;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.DBHostConfig;
import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.util.PasswordAuthPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;


import com.actiontech.dble.util.Base64Decoder;
import com.actiontech.dble.util.Security;


import javax.crypto.Cipher;

/**
 * @author mycat
 */
public class MySQLDataSource extends PhysicalDatasource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDataSource.class);


    private final MySQLConnectionFactory factory;

    public MySQLDataSource(DBHostConfig config, DataHostConfig hostConfig,
                           boolean isReadNode) {
        super(config, hostConfig, isReadNode);
        this.factory = new MySQLConnectionFactory();

    }

    @Override
    public void createNewConnection(ResponseHandler handler, String schema) throws IOException {
        factory.make(this, handler, schema);
    }

    private long getClientFlags(boolean isConnectWithDB) {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        if (isConnectWithDB) {
            flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        }
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        // flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        // flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }


    private long getClientFlagSha(boolean isConnectWithDB) {
        int flag = 0;
        flag |= getClientFlags(isConnectWithDB);
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        flag += PasswordAuthPlugin.getClientFlagsExtendSha() << 16;
        return flag;
    }

    @Override
    public boolean testConnection(String schema) throws IOException {

        boolean isConnected = true;
        Socket socket = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            socket = new Socket(this.getConfig().getIp(), this.getConfig().getPort());
            socket.setSoTimeout(1000 * 20);
            socket.setReceiveBufferSize(32768);
            socket.setSendBufferSize(32768);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            in = new BufferedInputStream(socket.getInputStream(), 32768);
            out = new BufferedOutputStream(socket.getOutputStream(), 32768);

            /**
             * Phase 1: MySQL to client. Send handshake packet.
             */
            BinaryPacket bin1 = new BinaryPacket();
            bin1.read(in);

            if (bin1.getData()[0] == ErrorPacket.FIELD_COUNT) {
                ErrorPacket err = new ErrorPacket();
                err.read(bin1);
                throw new IOException(new String(err.getMessage(), StandardCharsets.UTF_8));
            }
            HandshakeV10Packet handshake = new HandshakeV10Packet();
            handshake.read(bin1);

            String authPluginName = new String(handshake.getAuthPluginName());
            if (authPluginName.equals(new String(HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN))) {
                /**
                 * Phase 2: client to MySQL. Send auth packet.
                 */
                AuthPacket authPacket = new AuthPacket();
                authPacket.setPacketId(1);
                authPacket.setClientFlags(getClientFlags(schema != null));
                authPacket.setMaxPacketSize(1024 * 1024 * 16);
                authPacket.setCharsetIndex(handshake.getServerCharsetIndex() & 0xff);
                authPacket.setUser(this.getConfig().getUser());
                try {
                    authPacket.setPassword(PasswordAuthPlugin.passwd(this.getConfig().getPassword(), handshake));
                } catch (NoSuchAlgorithmException e) {
                    throw new IOException(e.getMessage());
                }
                authPacket.setDatabase(schema);
                authPacket.write(out);
                out.flush();

                /**
                 * Phase 3: MySQL to client. send OK/ERROR packet.
                 */
                BinaryPacket bin2 = new BinaryPacket();
                bin2.read(in);
                switch (bin2.getData()[0]) {
                    case OkPacket.FIELD_COUNT:
                        break;
                    case ErrorPacket.FIELD_COUNT:
                        isConnected = false;
                        break;
                    case EOFPacket.FIELD_COUNT:
                        // send 323 auth packet
                        Reply323Packet r323 = new Reply323Packet();
                        r323.setPacketId((byte) (bin2.getPacketId() + 1));
                        String password = this.getConfig().getPassword();
                        if (password != null && password.length() > 0) {
                            r323.setSeed(SecurityUtil.scramble323(password, new String(handshake.getSeed())).getBytes());
                        }
                        r323.write(out);
                        out.flush();
                        break;
                    default:
                        isConnected = false;
                        break;
                }
            } else if (authPluginName.equals(new String(HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN))) {
                /**
                 * Phase 2: client to MySQL. Send auth packet.
                 */
                AuthPacket authPacket = new AuthPacket();
                authPacket.setPacketId(1);
                authPacket.setClientFlags(getClientFlagSha(schema != null));
                authPacket.setMaxPacketSize(1024 * 1024 * 16);
                authPacket.setCharsetIndex(handshake.getServerCharsetIndex() & 0xff);
                authPacket.setUser(this.getConfig().getUser());
                authPacket.setAuthPlugin(authPluginName);
                try {
                    authPacket.setPassword(PasswordAuthPlugin.passwdSha256(this.getConfig().getPassword(), handshake));
                    authPacket.setDatabase(schema);
                    authPacket.writeWithKey(out);
                    out.flush();


                    BinaryPacket bin2 = new BinaryPacket();
                    bin2.read(in);
                    if (bin2.getData()[1] == 0x03) {        //fast Authentication
                        isConnected = true;
                    } else if (bin2.getData()[1] == 0x04) {   //full Authentication
                        out.write(QuitPacket.GETPUBLICKEY);
                        out.flush();
                        byte[] publicKey = null;
                        BinaryPacket binKey = new BinaryPacket();
                        binKey.readKey(in);
                        publicKey = binKey.getPublicKey();
                        byte[] input = this.getConfig().getPassword() != null ? getBytesNullTerminated(this.getConfig().getPassword(), "UTF-8") : new byte[]{0};
                        byte[] mysqlScrambleBuff = new byte[input.length];
                        Security.xorString(input, mysqlScrambleBuff, PasswordAuthPlugin.seedTotal, input.length);
                        byte[] encryptedPassword = PasswordAuthPlugin.encryptWithRSAPublicKey(mysqlScrambleBuff, PasswordAuthPlugin.decodeRSAPublicKey(new String(publicKey)), "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
                        byte[] encryPacketHead = {(byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x05};
                        out.write(encryPacketHead);
                        out.write(encryptedPassword);
                        out.flush();
                        BinaryPacket resEncryBin = new BinaryPacket();
                        resEncryBin.read(in);
                        byte[] resEncResult = resEncryBin.getData();
                        if (resEncResult[4] == 0x00) {
                            isConnected = true;
                        } else {
                            isConnected = false;
                        }
                    } else {
                        isConnected = false;
                    }
                } catch (Exception e) {
                    LOGGER.debug("connect the schema:" + schema + " failed");
                }
            } else {
                LOGGER.info("Client don't support the password plugin " + authPluginName);
                isConnected = false;
            }
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
        } finally {
            try {
                if (in != null)
                    in.close();
                if (out != null)
                    out.close();
                if (socket != null)
                    socket.close();
            } catch (Exception e) {
                //ignore error
            }
        }
        return isConnected;
    }

    public static byte[] getBytesNullTerminated(String value, String encoding) {
        // Charset cs = findCharset(encoding);
        Charset cs = StandardCharsets.UTF_8;
        ByteBuffer buf = cs.encode(value);
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen + 1];
        buf.get(asBytes, 0, encodedLen);
        asBytes[encodedLen] = 0;
        return asBytes;
    }
    @Override
    public DBHeartbeat createHeartBeat() {
        return new MySQLHeartbeat(this);
    }
}
