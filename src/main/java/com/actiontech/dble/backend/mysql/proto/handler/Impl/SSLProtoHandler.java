package com.actiontech.dble.backend.mysql.proto.handler.Impl;

import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;

import java.nio.ByteBuffer;

import static com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResultCode.*;

public class SSLProtoHandler implements ProtoHandler {

    public static final int PACKET_HEADER_SIZE = 5;

    @Override
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int offset, boolean isSupportCompress) {
        int position = dataBuffer.position();
        int length = getPacketLength(dataBuffer, offset);
        final ProtoHandlerResult.ProtoHandlerResultBuilder builder = ProtoHandlerResult.builder();
        return getProtoHandlerResultBuilder(dataBuffer, offset, position, length, builder).build();
    }

    private int getPacketLength(ByteBuffer buffer, int offset) {
        int packetLength = 0;
        if (buffer.position() >= offset + PACKET_HEADER_SIZE) {
            // SSLv3 or TLS - Check ContentType
            boolean tls;
            int packageType = buffer.get(offset) & 0xff;
            switch (packageType) {
                case 20:  // change_cipher_spec
                case 21:  // alert
                case 22:  // handshake
                case 23:  // application_data
                    tls = true;
                    break;
                default:
                    // SSLv2 or bad data
                    tls = false;
            }

            if (tls) {
                // SSLv3 or TLS - Check ProtocolVersion
                int majorVersion = buffer.get(offset + 1);
                if (majorVersion == 3) {
                    // SSLv3 or TLS
                    packetLength = buffer.getShort(offset + 3) & 0xffff;
                    packetLength += PACKET_HEADER_SIZE;
                    if (packetLength <= 5) {
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
                boolean sslv2 = true;
                int headerLength = (buffer.get(offset) & 0x80) != 0 ? 2 : 3;
                int majorVersion = buffer.get(offset + headerLength + 1);
                if (majorVersion == 2 || majorVersion == 3) {
                    // SSLv2
                    if (headerLength == 2) {
                        packetLength = (buffer.getShort(offset) & 0x7FFF) + 2;
                    } else {
                        packetLength = (buffer.getShort(offset) & 0x3FFF) + 3;
                    }
                    if (packetLength <= headerLength) {
                        sslv2 = false;
                    }
                } else {
                    sslv2 = false;
                }

                if (!sslv2) {
                    return -1;
                }
            }
            return packetLength;
        }
        return -1;
    }

    private ProtoHandlerResult.ProtoHandlerResultBuilder getProtoHandlerResultBuilder(ByteBuffer dataBuffer, int offset, int position, int length, ProtoHandlerResult.ProtoHandlerResultBuilder builder) {
        if (length == -1) {
            if (offset != 0) {
                return builder.setCode(BUFFER_PACKET_UNCOMPLETE).setHasMorePacket(false).setOffset(offset);
            } else if (!dataBuffer.hasRemaining()) {
                throw new RuntimeException("invalid dataBuffer capacity ,too little buffer size " +
                        dataBuffer.capacity());
            }
            return builder.setCode(BUFFER_PACKET_UNCOMPLETE).setHasMorePacket(false).setOffset(offset);
        }
        if (position >= offset + length) {
            dataBuffer.position(offset);
            byte[] data = new byte[length];
            dataBuffer.get(data, 0, length);

            builder.setCode(data[0] == 23 ? SSL_APP_PACKET : SSL_PROTO_PACKET);

            offset += length;
            if (position != offset) {
                dataBuffer.position(position);
                builder.setHasMorePacket(true);
            } else {
                builder.setHasMorePacket(false);
            }
            builder.setOffset(offset).setPacketData(data);
            return builder;
        } else {
            if (!dataBuffer.hasRemaining()) {
                return builder.setCode(SSL_BUFFER_NOT_BIG_ENOUGH).setHasMorePacket(false).setOffset(offset).setPacketLength(length);
            } else {
                return builder.setCode(BUFFER_PACKET_UNCOMPLETE).setHasMorePacket(false).setOffset(offset).setPacketLength(length);
            }
        }
    }
}
