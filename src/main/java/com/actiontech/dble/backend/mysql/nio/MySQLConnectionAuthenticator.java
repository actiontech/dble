/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.util.PasswordAuthPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * MySQLConnectionAuthenticator
 *
 * @author mycat
 */
public class MySQLConnectionAuthenticator implements NIOHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnectionAuthenticator.class);
    private final MySQLConnection source;
    private final ResponseHandler listener;
    private byte[] publicKey = null;
    private String authPluginName = null;
    private byte[] authPluginData = null;


    public MySQLConnectionAuthenticator(MySQLConnection source,
                                        ResponseHandler listener) {
        this.source = source;
        this.listener = listener;
    }

    public void connectionError(MySQLConnection c, Throwable e) {
        if (listener != null) {
            listener.connectionError(e, c);
        }
    }

    @Override
    public void handle(byte[] data) {
        try {
            BinaryPacket bin2 = new BinaryPacket();
            if (checkPubicKey(data)) {
                publicKey = bin2.readKey(data);
                if (Arrays.equals(source.getHandshake().getAuthPluginName(), HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN)) {
                    PasswordAuthPlugin.sendEnPasswordWithPublicKey(authPluginData, PasswordAuthPlugin.GETPUBLICKEY, publicKey, source);
                } else if (Arrays.equals(source.getHandshake().getAuthPluginName(), HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN)) {
                    PasswordAuthPlugin.sendEnPasswordWithPublicKey(authPluginData, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST, publicKey, source);
                } else {
                    LOGGER.warn("Client don't support the password plugin " + authPluginName + ",please check the default auth Plugin");
                }
            }
            switch (data[4]) {
                case OkPacket.FIELD_COUNT:
                    HandshakeV10Packet packet = source.getHandshake();
                    if (packet == null) {
                        processHandShakePacket(data);
                        // send auth packet
                        source.authenticate();
                        break;
                    }
                    // execute auth response
                    source.setHandler(new MySQLConnectionHandler(source));
                    source.setAuthenticated(true);
                    boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & packet.getServerCapabilities());
                    boolean usingCompress = DbleServer.getInstance().getConfig().getSystem().getUseCompression() == 1;
                    if (clientCompress && usingCompress) {
                        source.setSupportCompress(true);
                    }
                    if (listener != null) {
                        listener.connectionAcquired(source);
                    }
                    break;
                case ErrorPacket.FIELD_COUNT:
                    ErrorPacket err = new ErrorPacket();
                    err.read(data);
                    String errMsg = new String(err.getMessage());
                    LOGGER.warn("can't connect to mysql server ,errMsg:" + errMsg + " " + source);
                    //source.close(errMsg);
                    throw new ConnectionException(err.getErrNo(), errMsg);
                case EOFPacket.FIELD_COUNT:
                    authPluginName = bin2.getAuthPluginName(data);
                    authPluginData = bin2.getAuthPluginData(data);
                    if (authPluginName.equals(new String(HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN))) {
                        source.write(PasswordAuthPlugin.nativePassword(PasswordAuthPlugin.passwd(source.getPassword(), source.getHandshake())));
                    } else if (authPluginName.equals(new String(HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN))) {
                        source.write(PasswordAuthPlugin.cachingSha2Password(PasswordAuthPlugin.passwdSha256(source.getPassword(), source.getHandshake())));
                    } else {
                        LOGGER.warn("Client don't support the MySQL 323 plugin ");
                        auth323(data[3]);
                    }
                    break;
                case PasswordAuthPlugin.AUTH_SWITCH_PACKET:
                    if (data[5] == PasswordAuthPlugin.AUTHSTAGE_FULL) {
                        if (Arrays.equals(source.getHandshake().getAuthPluginName(), HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN)) {
                            PasswordAuthPlugin.sendEnPaGetPub(PasswordAuthPlugin.GETPUBLICKEY, source);
                        } else if (Arrays.equals(source.getHandshake().getAuthPluginName(), HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN)) {
                            PasswordAuthPlugin.sendEnPaGetPub(PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST, source);
                        } else {
                            LOGGER.warn("Client don't support the password plugin " + authPluginName + ",please check the default auth Plugin");
                        }
                    }
                    break;
                default:
                    packet = source.getHandshake();
                    if (packet == null) {
                        processHandShakePacket(data);
                        // send auth packet
                        source.authenticate();
                        break;
                    } else {
                        throw new RuntimeException("Unknown Packet!");
                    }

            }

        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
            if (listener != null) {
                listener.connectionError(e, source);
                return;
            }
        }
    }

    private void processHandShakePacket(byte[] data) {
        HandshakeV10Packet packet = new HandshakeV10Packet();
        packet.read(data);
        source.setHandshake(packet);
        source.setThreadId(packet.getThreadId());

        source.initCharacterSet(DbleServer.getInstance().getConfig().getSystem().getCharset());
    }

    private void auth323(byte packetId) {
        // send 323 auth packet
        Reply323Packet r323 = new Reply323Packet();
        r323.setPacketId(++packetId);
        String pass = source.getPassword();
        if (pass != null && pass.length() > 0) {
            byte[] seed = source.getHandshake().getSeed();
            r323.setSeed(SecurityUtil.scramble323(pass, new String(seed)).getBytes());
        }
        r323.write(source);
    }

    public boolean checkPubicKey(byte[] data) {
        return data[0] == (byte) 0xc4 && data[1] == (byte) 1 && data[2] == (byte) 0 && (data[3] == (byte) 4 || data[3] == (byte) 6);
    }

}
