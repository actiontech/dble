/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.DataSourceConfig;
import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.util.PasswordAuthPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author mycat
 */
public class MySQLDataSource extends PhysicalDataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDataSource.class);
    private final MySQLConnectionFactory factory;

    public MySQLDataSource(DataSourceConfig config, DataHostConfig hostConfig,
                           boolean isReadNode) {
        super(config, hostConfig, isReadNode);
        this.factory = new MySQLConnectionFactory();
    }

    public MySQLDataSource(MySQLDataSource org) {
        super(org);
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
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        flag |= Capabilities.CLIENT_MULTIPLE_STATEMENTS;
        return flag;
    }

    @Override
    public boolean testConnection() throws IOException {

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
            this.setDsVersion(new String(handshake.getServerVersion()));
            byte[] authPluginData = null;
            if (authPluginName.equals(new String(HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN))) {
                /**
                 * Phase 2: client to MySQL. Send auth packet.
                 */
                startAuthPacket(out, handshake, PasswordAuthPlugin.passwd(this.getConfig().getPassword(), handshake), authPluginName);
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
                        logTestConnectionError(bin2.getData());
                        break;
                    case EOFPacket.FIELD_COUNT:
                        authPluginName = bin2.getAuthPluginName();
                        authPluginData = bin2.getAuthPluginData();
                        if (authPluginName.equals(new String(HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN))) {
                            out.write(PasswordAuthPlugin.cachingSha2Password(PasswordAuthPlugin.passwdSha256(this.getConfig().getPassword(), handshake)));
                            out.flush();
                            bin2.read(in);
                            if (bin2.getData()[0] == ErrorPacket.FIELD_COUNT) {
                                isConnected = false;
                                logTestConnectionError(bin2.getData());
                                break;
                            }
                            if (bin2.getData()[1] == PasswordAuthPlugin.AUTHSTAGE_FAST_COMPLETE) {       //fast Authentication
                                break;
                            } else if (bin2.getData()[1] == PasswordAuthPlugin.AUTHSTAGE_FULL) {  //full Authentication
                                isConnected = PasswordAuthPlugin.sendEncryptedPassword(out, in, authPluginData, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST, this.getConfig().getPassword());
                            } else {
                                isConnected = false;
                            }
                        } else {
                            // send 323 auth packet
                            isConnected = false;
                            LOGGER.warn("Client don't support the MySQL 323 plugin ");
                            PasswordAuthPlugin.send323AuthPacket(out, bin2, handshake, this.getConfig().getPassword());
                        }
                        break;
                    default:
                        isConnected = false;
                        break;
                }
            } else if (authPluginName.equals(new String(HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN))) {
                /**
                 * Phase 2: client to MySQL. Send auth packet.
                 */
                try {
                    startAuthPacket(out, handshake, PasswordAuthPlugin.passwdSha256(this.getConfig().getPassword(), handshake), authPluginName);

                    BinaryPacket bin2 = new BinaryPacket();
                    bin2.read(in);
                    switch (bin2.getData()[0]) {
                        case OkPacket.FIELD_COUNT:
                            break;
                        case PasswordAuthPlugin.AUTH_SWITCH_PACKET:
                            if (bin2.getData()[1] == PasswordAuthPlugin.AUTHSTAGE_FAST_COMPLETE) {        //fast Authentication
                                break;
                            } else if (bin2.getData()[1] == PasswordAuthPlugin.AUTHSTAGE_FULL) {   //full Authentication
                                isConnected = PasswordAuthPlugin.sendEncryptedPassword(out, in, authPluginData, PasswordAuthPlugin.GETPUBLICKEY, this.getConfig().getPassword());
                            } else {
                                isConnected = false;
                            }
                            break;
                        case ErrorPacket.FIELD_COUNT:
                            isConnected = false;
                            logTestConnectionError(bin2.getData());
                            break;
                        case EOFPacket.FIELD_COUNT:
                            authPluginName = bin2.getAuthPluginName();
                            if (authPluginName.equals(new String(HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN))) {
                                out.write(PasswordAuthPlugin.nativePassword(PasswordAuthPlugin.passwd(this.getConfig().getPassword(), handshake)));
                                out.flush();
                                bin2.read(in);
                                if (bin2.getData()[0] == ErrorPacket.FIELD_COUNT) {
                                    isConnected = false;
                                    logTestConnectionError(bin2.getData());
                                }
                            } else {
                                // send 323 auth packet
                                isConnected = false;
                                LOGGER.warn("Client don't support the MySQL 323 plugin ");
                                PasswordAuthPlugin.send323AuthPacket(out, bin2, handshake, this.getConfig().getPassword());
                            }
                            break;
                        default:
                            isConnected = false;
                            break;
                    }
                } catch (Exception e) {
                    LOGGER.warn("testConnection failed");
                    isConnected = false;
                }
            } else {
                LOGGER.warn("Client don't support the password plugin " + authPluginName + ",please check the default auth Plugin");
                isConnected = false;
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
            isConnected = false;
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

    @Override
    public MySQLHeartbeat createHeartBeat() {
        return new MySQLHeartbeat(this);
    }


    public void startAuthPacket(OutputStream out, HandshakeV10Packet handshake, byte[] passwordSented, String authPluginName) {
        AuthPacket authPacket = new AuthPacket();
        authPacket.setPacketId(1);
        authPacket.setClientFlags(getClientFlagSha(false));
        authPacket.setMaxPacketSize(1024 * 1024 * 16);
        authPacket.setCharsetIndex(handshake.getServerCharsetIndex() & 0xff);
        authPacket.setUser(this.getConfig().getUser());
        try {
            authPacket.setPassword(passwordSented);
            authPacket.setDatabase(null);
            authPacket.setAuthPlugin(authPluginName);
            authPacket.writeWithKey(out);
            out.flush();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
        }
    }

    public static void logTestConnectionError(byte[] errorData) {
        ErrorPacket err = new ErrorPacket();
        err.read(errorData);
        String errMsg = new String(err.getMessage());
        LOGGER.warn("can't connect to mysql server ,errMsg:" + errMsg + " ");
    }
}
