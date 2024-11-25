/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.proto.handler.Impl;

import com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandler;
import com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandlerResult;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.net.ssl.GMSslWrapper;
import com.oceanbase.obsharding_d.net.ssl.OpenSSLWrapper;
import com.oceanbase.obsharding_d.util.exception.NotSslRecordException;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

import static com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandlerResultCode.*;


/**
 * Created by szf on 2020/6/16.
 */
public class MySQLProtoHandlerImpl implements ProtoHandler {

    private byte[] incompleteData = null;
    /**
     * the length of the ssl record header (in bytes)
     */
    static final int SSL_RECORD_HEADER_LENGTH = 5;
    /**
     * change cipher spec
     */
    static final int SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC = 20;

    /**
     * alert
     */
    static final int SSL_CONTENT_TYPE_ALERT = 21;

    /**
     * handshake
     */
    static final int SSL_CONTENT_TYPE_HANDSHAKE = 22;

    /**
     * application data
     */
    static final int SSL_CONTENT_TYPE_APPLICATION_DATA = 23;

    /**
     * HeartBeat Extension
     * <p>
     * jdk not support. see:sun.security.ssl.ContentType
     * <p>
     * can cause <a href="https://en.wikipedia.org/wiki/Heartbleed">Heartbleed</a>
     */
    @Deprecated
    static final int SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT = 24;
    /**
     * Not enough data in buffer to parse the record length
     */
    static final int NOT_ENOUGH_DATA = -1;

    /**
     * data is not encrypted
     */
    static final int NOT_ENCRYPTED = -2;
    /**
     * GMSSL Protocol Version
     */
    static final int GMSSL_PROTOCOL_VERSION = 0x101;
    private final AbstractConnection connection;
    private int protocol = OpenSSLWrapper.PROTOCOL;

    public MySQLProtoHandlerImpl(AbstractConnection connection) {
        this.connection = connection;
    }

    @Override
    @Nonnull
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int offset, boolean isSupportCompress, boolean isContainSSLData) throws NotSslRecordException {
        int position = dataBuffer.position();
        boolean isSSL = false;
        //get length
        int length;
        if (isContainSSLData) {
            if ((length = getSSLPacketLength(dataBuffer, offset)) != NOT_ENOUGH_DATA && length != NOT_ENCRYPTED) {
                //client hello
                isSSL = true;
            } else {
                //login
                length = getNonSSLPacketLength(dataBuffer, offset, isSupportCompress);
            }
        } else {
            length = getNonSSLPacketLength(dataBuffer, offset, isSupportCompress);
        }


        final ProtoHandlerResult.ProtoHandlerResultBuilder builder = ProtoHandlerResult.builder();
        ProtoHandlerResult.ProtoHandlerResultBuilder resultBuilder = getProtoHandlerResultBuilder(dataBuffer, offset, position, length, builder, isSSL);
        if (connection != null && resultBuilder.getCode().equals(SSL_PROTO_PACKET)) {
            connection.initSSLContext(protocol);
        }
        return resultBuilder.build();
    }

    protected int getSSLPacketLength(ByteBuffer buffer, int offset) {
        if (buffer.position() < offset + SSL_RECORD_HEADER_LENGTH) {
            return NOT_ENOUGH_DATA;
        }
        int packetLength = 0;
        // SSLv3 or TLS - Check ContentType
        boolean tls;
        switch (buffer.get(offset) & 0xff) {
            case SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC:
            case SSL_CONTENT_TYPE_ALERT:
            case SSL_CONTENT_TYPE_HANDSHAKE:
            case SSL_CONTENT_TYPE_APPLICATION_DATA:
                tls = true;
                break;
            default:
                // SSLv2 or bad data
                tls = false;
        }

        if (tls) {
            // SSLv3 or TLS or GMSSLv1.0 or GMSSLv1.1 - Check ProtocolVersion
            int majorVersion = buffer.get(offset + 1);
            if (majorVersion == 3 || buffer.getShort(offset + 1) == GMSSL_PROTOCOL_VERSION) {
                if (buffer.getShort(offset + 1) == GMSSL_PROTOCOL_VERSION) {
                    protocol = GMSslWrapper.PROTOCOL;
                }
                // SSLv3 or TLS or GMSSLv1.0 or GMSSLv1.1
                packetLength = (buffer.getShort(offset + 3) & 0xffff) + SSL_RECORD_HEADER_LENGTH;
                if (packetLength <= SSL_RECORD_HEADER_LENGTH) {
                    // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                    tls = false;
                }
            } else {
                // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                tls = false;
            }
        }

        if (!tls) {
            // SSLv2 or bad data - Check the version
            int headerLength = (buffer.get(offset) & 0x80) != 0 ? 2 : 3;
            int majorVersion = buffer.get(offset + headerLength + 1);
            if (majorVersion == 2 || majorVersion == 3) {
                // SSLv2
                packetLength = headerLength == 2 ? (buffer.getShort(offset) & 0x7FFF) + 2 : (buffer.getShort(offset) & 0x3FFF) + 3;
                if (packetLength <= headerLength) {
                    return NOT_ENOUGH_DATA;
                }
            } else {
                return NOT_ENCRYPTED;
            }
        }
        return packetLength;
    }

    @Nonnull
    public ProtoHandlerResult.ProtoHandlerResultBuilder handlerResultBuilder(ByteBuffer dataBuffer, int offset, boolean isSupportCompress) {
        int position = dataBuffer.position();
        int length = getNonSSLPacketLength(dataBuffer, offset, isSupportCompress);
        final ProtoHandlerResult.ProtoHandlerResultBuilder builder = ProtoHandlerResult.builder();
        return getProtoHandlerResultBuilder(dataBuffer, offset, position, length, builder, false);
    }

    private ProtoHandlerResult.ProtoHandlerResultBuilder getProtoHandlerResultBuilder(ByteBuffer dataBuffer, int offset, int position, int length,
                                                                                      ProtoHandlerResult.ProtoHandlerResultBuilder builder, boolean isSSL) {
        if (length == -1) {
            if (offset != 0) {
                return builder.setCode(BUFFER_PACKET_UNCOMPLETE).setHasMorePacket(false).setOffset(offset);
            } else if (!dataBuffer.hasRemaining()) {
                throw new RuntimeException("invalid dataBuffer capacity ,too little buffer size " + dataBuffer.capacity());
            }
            return builder.setCode(BUFFER_PACKET_UNCOMPLETE).setHasMorePacket(false).setOffset(offset);
        }
        if (position >= offset + length) {
            // handle this package
            dataBuffer.position(offset);
            byte[] data = new byte[length];
            dataBuffer.get(data, 0, length);
            data = checkData(data, length);
            if (data == null) {
                builder.setCode(PART_OF_BIG_PACKET);
            } else {
                builder.setCode(isSSL ? SSL_PROTO_PACKET : COMPLETE_PACKET);
            }
            // offset to next position
            offset += length;
            // reached end
            if (position != offset) {
                // try next package parse
                //dataBufferOffset = offset;
                //should reset position after read.
                dataBuffer.position(position);
                builder.setHasMorePacket(true);
            } else {
                builder.setHasMorePacket(false);
            }
            builder.setOffset(offset).setPacketData(data);
            return builder;
        } else {
            // not read whole message package ,so check if buffer enough and
            // compact dataBuffer
            if (!dataBuffer.hasRemaining()) {
                return builder.setCode(isSSL ? SSL_BUFFER_NOT_BIG_ENOUGH : BUFFER_NOT_BIG_ENOUGH).setHasMorePacket(false).setOffset(offset).setPacketLength(length);
            } else {
                return builder.setCode(BUFFER_PACKET_UNCOMPLETE).setHasMorePacket(false).setOffset(offset).setPacketLength(length);
            }
        }
    }


    private int getNonSSLPacketLength(ByteBuffer buffer, int offset, boolean isSupportCompress) {
        int headerSize = MySQLPacket.PACKET_HEADER_SIZE;
        if (isSupportCompress) {
            headerSize = 7;
        }

        if (buffer.position() < offset + headerSize) {
            return -1;
        } else {
            int length = buffer.get(offset) & 0xff;
            length |= (buffer.get(++offset) & 0xff) << 8;
            length |= (buffer.get(++offset) & 0xff) << 16;
            return length + headerSize;
        }
    }


    private byte[] checkData(byte[] data, int length) {
        //session packet should be set to the latest one
        //todo: this method doesn't apply for compress
        if (length >= com.oceanbase.obsharding_d.net.mysql.MySQLPacket.MAX_PACKET_SIZE + com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE) {
            if (incompleteData == null) {
                incompleteData = data;
            } else {
                //skip header in package
                byte[] nextData = new byte[data.length - com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE];
                System.arraycopy(data, com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE, nextData, 0, data.length - com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE);
                incompleteData = dataMerge(nextData);
            }
            return null;
        } else {
            if (incompleteData != null) {
                byte[] nextData = new byte[data.length - com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE];
                System.arraycopy(data, com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE, nextData, 0, data.length - com.oceanbase.obsharding_d.net.mysql.MySQLPacket.PACKET_HEADER_SIZE);
                incompleteData = dataMerge(nextData);
                data = incompleteData;
                incompleteData = null;
            }
            return data;
        }
    }

    private byte[] dataMerge(byte[] data) {
        //todo:  could optimize here. for example ,use linked-buffer
        byte[] newData = new byte[incompleteData.length + data.length];
        System.arraycopy(incompleteData, 0, newData, 0, incompleteData.length);
        System.arraycopy(data, 0, newData, incompleteData.length, data.length);
        return newData;
    }

    public int getProtocol() {
        return protocol;
    }
}
