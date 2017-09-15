/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQLConnectionAuthenticator
 *
 * @author mycat
 */
public class MySQLConnectionAuthenticator implements NIOHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnectionAuthenticator.class);
    private final MySQLConnection source;
    private final ResponseHandler listener;

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
                    LOGGER.warn("can't connect to mysql server ,errmsg:" + errMsg + " " + source);
                    //source.close(errMsg);
                    throw new ConnectionException(err.getErrno(), errMsg);

                case EOFPacket.FIELD_COUNT:
                    auth323(data[3]);
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

        } catch (RuntimeException e) {
            if (listener != null) {
                listener.connectionError(e, source);
                return;
            }
            throw e;
        }
    }

    private void processHandShakePacket(byte[] data) {
        HandshakeV10Packet packet = new HandshakeV10Packet();
        packet.read(data);
        source.setHandshake(packet);
        source.setThreadId(packet.getThreadId());

        int charsetIndex = (packet.getServerCharsetIndex() & 0xff);
        String charset = CharsetUtil.getCharset(charsetIndex);
        if (charset != null) {
            source.setCharacterSet(charset);
        } else {
            throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
        }
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

}
