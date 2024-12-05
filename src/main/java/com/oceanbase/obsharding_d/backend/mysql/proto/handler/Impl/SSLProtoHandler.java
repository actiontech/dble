/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.proto.handler.Impl;

import com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandlerResult;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.util.exception.NotSslRecordException;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

import static com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandlerResultCode.*;

public class SSLProtoHandler extends MySQLProtoHandlerImpl {

    public SSLProtoHandler(AbstractConnection connection) {
        super(connection);
    }

    @NotNull
    @Override
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int offset, boolean isSupportCompress, boolean isContainSSLData) throws NotSslRecordException {
        int position = dataBuffer.position();
        int length = getSSLPacketLength(dataBuffer, offset);
        if (length == NOT_ENOUGH_DATA || length == NOT_ENCRYPTED) {
            throw new NotSslRecordException("not an SSL/TLS record");
        }
        final ProtoHandlerResult.ProtoHandlerResultBuilder builder = ProtoHandlerResult.builder();
        return getProtoHandlerResultBuilder(dataBuffer, offset, position, length, builder).build();
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
            switch (data[0]) {
                case 23:
                    builder.setCode(SSL_APP_PACKET);
                    break;
                case 21:
                    builder.setCode(SSL_CLOSE_PACKET);
                    break;
                default:
                    builder.setCode(SSL_PROTO_PACKET);
            }

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
