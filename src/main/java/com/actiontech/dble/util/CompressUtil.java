/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.AbstractConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


/**
 * compressed-packet
 * <p>
 * http://dev.mysql.com/doc/internals/en/compressed-packet-header.html
 * <p>
 * header
 * 3 Bytes   length of compressed payload
 * 1 Bytes   compressed sequence id
 * 3 Bytes   length of payload before compression
 * <p>
 * (body)
 * n Bytes   compressed content or uncompressed content
 * <p>
 * | -------------------------------------------------------------------------------------- |
 * | comp-length  |  seq-id  | uncomp-len   |                Compressed Payload             |
 * | ------------------------------------------------ ------------------------------------- |
 * |  22 00 00    |   00     |  32 00 00    | compress("\x2e\x00\x00\x00\x03select ...")    |
 * | -------------------------------------------------------------------------------------- |
 * <p>
 * Q:why body is compressed content or uncompressed content
 * A:Usually payloads less than 50 bytes (MIN_COMPRESS_LENGTH) aren't compressed.
 */
public final class CompressUtil {
    private CompressUtil() {
    }

    public static final int MINI_LENGTH_TO_COMPRESS = 50;
    public static final int NO_COMPRESS_PACKET_LENGTH = MINI_LENGTH_TO_COMPRESS + 4;


    /**
     * compressMysqlPacket
     *
     * @param input
     * @param con
     * @param compressUnfinishedDataQueue
     * @return
     */
    public static ByteBuffer compressMysqlPacket(ByteBuffer input, AbstractConnection con,
                                                 ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue) {

        byte[] byteArrayFromBuffer = getByteArrayFromBuffer(input);
        con.recycle(input);

        byteArrayFromBuffer = mergeBytes(byteArrayFromBuffer, compressUnfinishedDataQueue);
        return compressMysqlPacket(byteArrayFromBuffer, con, compressUnfinishedDataQueue);
    }


    /**
     * compressMysqlPacket
     *
     * @param data
     * @param con
     * @param compressUnfinishedDataQueue
     * @return
     */
    private static ByteBuffer compressMysqlPacket(byte[] data, AbstractConnection con,
                                                  ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue) {

        ByteBuffer byteBuf = con.allocate();
        byteBuf = con.checkWriteBuffer(byteBuf, data.length, false); //TODO: optimizer performance

        MySQLMessage msg = new MySQLMessage(data);
        while (msg.hasRemaining()) {

            //body length
            int packetLength = 0;

            //readable length
            int readLength = msg.length() - msg.position();
            if (readLength > 3) {
                packetLength = msg.readUB3();
                msg.move(-3);
            }

            // valid the data
            if (readLength < packetLength + 4) {
                byte[] packet = msg.readBytes(readLength);
                if (packet.length != 0) {
                    compressUnfinishedDataQueue.add(packet); //unfinished packet
                }
            } else {

                byte[] packet = msg.readBytes(packetLength + 4);
                if (packet.length != 0) {

                    if (packet.length <= NO_COMPRESS_PACKET_LENGTH) {
                        BufferUtil.writeUB3(byteBuf, packet.length);    //length of compressed payload
                        byteBuf.put(packet[3]);                            //compressed sequence id
                        BufferUtil.writeUB3(byteBuf, 0);                //length of payload before compression is 0
                        byteBuf.put(packet);                            //body

                    } else {

                        byte[] compress = compress(packet);                //compress

                        BufferUtil.writeUB3(byteBuf, compress.length);
                        byteBuf.put(packet[3]);
                        BufferUtil.writeUB3(byteBuf, packet.length);
                        byteBuf.put(compress);
                    }
                }
            }
        }
        return byteBuf;
    }

    /**
     * decompressMysqlPacket
     *
     * @param data
     * @param decompressUnfinishedDataQueue
     * @return
     */
    public static List<byte[]> decompressMysqlPacket(byte[] data,
                                                     ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue) {

        MySQLMessage msg = new MySQLMessage(data);

        //header
        //-----------------------------------------
        int packetLength = msg.readUB3();  //length of compressed payload
        msg.read();           //compressed sequence id
        int oldLen = msg.readUB3();           //length of payload before compression

        //return if not compress
        if (packetLength == data.length - 4) {
            List<byte[]> lst = new ArrayList(1);
            lst.add(data);
            return lst;
            //compressed failed
        } else if (oldLen == 0) {
            byte[] readBytes = msg.readBytes();
            return splitPack(readBytes, decompressUnfinishedDataQueue);

            //decompress
        } else {
            byte[] de = decompress(data, 7, data.length - 7);
            return splitPack(de, decompressUnfinishedDataQueue);
        }
    }

    /**
     * splitPack
     *
     * @param in
     * @param decompressUnfinishedDataQueue
     * @return
     */
    private static List<byte[]> splitPack(byte[] in, ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue) {

        //merge
        in = mergeBytes(in, decompressUnfinishedDataQueue);

        List<byte[]> smallPackList = new ArrayList<>();

        MySQLMessage msg = new MySQLMessage(in);
        while (msg.hasRemaining()) {

            int readLength = msg.length() - msg.position();
            int packetLength = 0;
            if (readLength > 3) {
                packetLength = msg.readUB3();
                msg.move(-3);
            }

            if (readLength < packetLength + 4) {
                byte[] packet = msg.readBytes(readLength);
                if (packet.length != 0) {
                    decompressUnfinishedDataQueue.add(packet);
                }

            } else {
                byte[] packet = msg.readBytes(packetLength + 4);
                if (packet.length != 0) {
                    smallPackList.add(packet);
                }
            }
        }

        return smallPackList;
    }

    /**
     * merge
     */
    private static byte[] mergeBytes(byte[] in, ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue) {

        if (!decompressUnfinishedDataQueue.isEmpty()) {

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                while (!decompressUnfinishedDataQueue.isEmpty()) {
                    out.write(decompressUnfinishedDataQueue.poll());
                }
                out.write(in);
                in = out.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    out.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }
        return in;
    }

    private static byte[] getByteArrayFromBuffer(ByteBuffer byteBuf) {
        byteBuf.flip();
        byte[] row = new byte[byteBuf.limit()];
        byteBuf.get(row);
        byteBuf.clear();
        return row;
    }

    public static byte[] compress(ByteBuffer byteBuf) {
        return compress(getByteArrayFromBuffer(byteBuf));
    }

    /**
     * use zlib to compress
     *
     * @param data
     * @return
     */
    public static byte[] compress(byte[] data) {

        byte[] output = null;

        Deflater compresser = new Deflater();
        compresser.setInput(data);
        compresser.finish();

        ByteArrayOutputStream out = new ByteArrayOutputStream(data.length);
        byte[] result = new byte[1024];
        try {
            while (!compresser.finished()) {
                int length = compresser.deflate(result);
                out.write(result, 0, length);
            }
            output = out.toByteArray();
        } finally {
            try {
                out.close();
            } catch (Exception e) {
                //ignore error
            }
            compresser.end();
        }

        return output;
    }

    /**
     * use zlib to decompress
     *
     * @param data data
     * @param off  offset
     * @param len  length
     * @return
     */
    public static byte[] decompress(byte[] data, int off, int len) {

        byte[] output = null;

        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(data, off, len);

        ByteArrayOutputStream out = new ByteArrayOutputStream(data.length);
        try {
            byte[] result = new byte[1024];
            while (!decompresser.finished()) {
                int i = decompresser.inflate(result);
                out.write(result, 0, i);
            }
            output = out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                out.close();
            } catch (Exception e) {
                //ignore error
            }
            decompresser.end();
        }
        return output;
    }

}
