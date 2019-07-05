package com.actiontech.dble.net.netty;

import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.util.TimeUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2019/7/3.
 */
public class MysqlNettyDelimiterDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        list.addAll(splitByMySQLProtocl(byteBuf));
    }

    private List<byte[]> splitByMySQLProtocl(ByteBuf byteBuf) {
        List<byte[]> list = new ArrayList<>();

        // read and split data in a loop
        int offset = 0, limit = byteBuf.writerIndex();
        for (; ; ) {
            int length = getPacketLength(byteBuf, offset);
            if (length == -1) {
                throw new RuntimeException("invalid readbuffer capacity ,too little buffer size " + byteBuf.capacity());
            }
            byte[] data = new byte[length];
            byteBuf.getBytes(offset, data);
            offset += length;
            list.add(data);
            if (limit == offset) {
                break;
            }
        }
        return list;
    }


    private int getPacketLength(ByteBuf buffer, int offset) {
        int headerSize = MySQLPacket.PACKET_HEADER_SIZE;
        //SupportCompress NO!!!!
        if (buffer.writerIndex() < offset + headerSize) {
            return -1;
        } else {
            int length = buffer.getInt(offset) & 0xff;
            length |= (buffer.getInt(++offset) & 0xff) << 8;
            length |= (buffer.getInt(++offset) & 0xff) << 16;
            return length + headerSize;
        }
    }
}
