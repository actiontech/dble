package com.actiontech.dble.backend.mysql.proto.handler.Impl;

import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.net.mysql.MySQLPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResultCode.*;


/**
 * Created by szf on 2020/6/16.
 */
public class MySQLProtoHandlerImpl implements ProtoHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtoHandlerImpl.class);

    private byte[] incompleteData = null;

    public MySQLProtoHandlerImpl() {
    }

    @Override
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int offset, boolean isSupportCompress) {
        int position = dataBuffer.position();
        int length = getPacketLength(dataBuffer, offset, isSupportCompress);
        if (length == -1) {
            if (offset != 0) {
                return new ProtoHandlerResult(BUFFER_PACKET_UNCOMPLETE, offset);
            } else if (!dataBuffer.hasRemaining()) {
                throw new RuntimeException("invalid dataBuffer capacity ,too little buffer size " +
                        dataBuffer.capacity());
            }
            return new ProtoHandlerResult(REACH_END_BUFFER, offset);
        }
        if (position >= offset + length) {
            // handle this package
            dataBuffer.position(offset);
            byte[] data = new byte[length];
            dataBuffer.get(data, 0, length);
            data = checkData(data, length);
            if (data == null) {
                return new ProtoHandlerResult(REACH_END_BUFFER, offset);
            }

            // offset to next position
            offset += length;
            // reached end
            if (position != offset) {
                // try next package parse
                //dataBufferOffset = offset;
                dataBuffer.position(position);
                return new ProtoHandlerResult(STLL_DATA_REMING, offset, data);
            }
            return new ProtoHandlerResult(REACH_END_BUFFER, offset, data);
        } else {
            // not read whole message package ,so check if buffer enough and
            // compact dataBuffer
            if (!dataBuffer.hasRemaining()) {
                return new ProtoHandlerResult(BUFFER_NOT_BIG_ENOUGH, offset, length);
            } else {
                return new ProtoHandlerResult(BUFFER_PACKET_UNCOMPLETE, offset, length);
            }
        }
    }


    private int getPacketLength(ByteBuffer buffer, int offset, boolean isSupportCompress) {
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
        if (length >= com.actiontech.dble.net.mysql.MySQLPacket.MAX_PACKET_SIZE + com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE) {
            if (incompleteData == null) {
                incompleteData = data;
            } else {
                byte[] nextData = new byte[data.length - com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE];
                System.arraycopy(data, com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE, nextData, 0, data.length - com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE);
                incompleteData = dataMerge(nextData);
            }
            return null;
        } else {
            if (incompleteData != null) {
                byte[] nextData = new byte[data.length - com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE];
                System.arraycopy(data, com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE, nextData, 0, data.length - com.actiontech.dble.net.mysql.MySQLPacket.PACKET_HEADER_SIZE);
                incompleteData = dataMerge(nextData);
                data = incompleteData;
                incompleteData = null;
            }
            return data;
        }
    }

    private byte[] dataMerge(byte[] data) {
        byte[] newData = new byte[incompleteData.length + data.length];
        System.arraycopy(incompleteData, 0, newData, 0, incompleteData.length);
        System.arraycopy(data, 0, newData, incompleteData.length, data.length);
        return newData;
    }
}
