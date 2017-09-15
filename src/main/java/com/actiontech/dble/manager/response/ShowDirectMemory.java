/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.memory.SeverMemory;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.memory.unsafe.utils.JavaUtils;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * show@@directmemory
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-21 17:35
 */

public final class ShowDirectMemory {
    private ShowDirectMemory() {
    }

    private static final int DETAIL_FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket DETAIL_HEADER = PacketUtil.getHeader(DETAIL_FIELD_COUNT);
    private static final FieldPacket[] DETAIL_FIELDS = new FieldPacket[DETAIL_FIELD_COUNT];
    private static final EOFPacket DETAIL_EOF = new EOFPacket();


    private static final int TOTAL_FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket TOTAL_HEADER = PacketUtil.getHeader(TOTAL_FIELD_COUNT);
    private static final FieldPacket[] TOTAL_FIELDS = new FieldPacket[TOTAL_FIELD_COUNT];
    private static final EOFPacket TOTAL_EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        DETAIL_HEADER.setPacketId(++packetId);

        DETAIL_FIELDS[i] = PacketUtil.getField("THREAD_ID", Fields.FIELD_TYPE_VAR_STRING);
        DETAIL_FIELDS[i++].setPacketId(++packetId);

        DETAIL_FIELDS[i] = PacketUtil.getField("MEM_USE_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        DETAIL_FIELDS[i++].setPacketId(++packetId);

        DETAIL_FIELDS[i] = PacketUtil.getField("  SIZE  ", Fields.FIELD_TYPE_VAR_STRING);
        DETAIL_FIELDS[i++].setPacketId(++packetId);
        DETAIL_EOF.setPacketId(++packetId);


        i = 0;
        packetId = 0;

        TOTAL_HEADER.setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("MDIRECT_MEMORY_MAXED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_USED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_AVAILABLE", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("SAFETY_FRACTION", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_RESERVED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);
        TOTAL_EOF.setPacketId(++packetId);


    }


    public static void execute(ManagerConnection c, int showtype) {

        if (showtype == 1) {
            showDirectMemoryTotal(c);
        } else if (showtype == 2) {
            showDirectMemoryDetail(c);
        }
    }


    public static void showDirectMemoryDetail(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = DETAIL_HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : DETAIL_FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = DETAIL_EOF.write(buffer, c, true);

        // write rows
        byte packetId = DETAIL_EOF.getPacketId();

        int useOffHeapForMerge = DbleServer.getInstance().getConfig().getSystem().getUseOffHeapForMerge();

        ConcurrentMap<Long, Long> networkbufferpool = DbleServer.getInstance().
                getBufferPool().getNetDirectMemoryUsage();

        if (useOffHeapForMerge == 1) {
            ConcurrentMap<Long, Long> map = DbleServer.getInstance().
                    getServerMemory().
                    getResultMergeMemoryManager().getDirectMemorUsage();
            for (Map.Entry<Long, Long> entry : map.entrySet()) {
                RowDataPacket row = new RowDataPacket(DETAIL_FIELD_COUNT);
                long value = entry.getValue();
                row.add(StringUtil.encode(String.valueOf(entry.getKey()), c.getCharset().getResults()));
                /**
                 * DIRECTMEMORY used by result
                 */
                row.add(StringUtil.encode("MergeMemoryPool", c.getCharset().getResults()));
                row.add(StringUtil.encode(value > 0 ? JavaUtils.bytesToString2(value) : "0", c.getCharset().getResults()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        for (Map.Entry<Long, Long> entry : networkbufferpool.entrySet()) {
            RowDataPacket row = new RowDataPacket(DETAIL_FIELD_COUNT);
            long value = entry.getValue();
            row.add(StringUtil.encode(String.valueOf(entry.getKey()), c.getCharset().getResults()));
            /**
             * DIRECTMEMORY belong to Buffer Pool
             */
            row.add(StringUtil.encode("NetWorkBufferPool", c.getCharset().getResults()));
            row.add(StringUtil.encode(value > 0 ? JavaUtils.bytesToString2(value) : "0", c.getCharset().getResults()));

            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }


    public static void showDirectMemoryTotal(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = TOTAL_HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : TOTAL_FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = TOTAL_EOF.write(buffer, c, true);
        // write rows
        byte packetId = TOTAL_EOF.getPacketId();

        int useOffHeapForMerge = DbleServer.getInstance().getConfig().
                getSystem().getUseOffHeapForMerge();

        ConcurrentMap<Long, Long> networkbufferpool = DbleServer.getInstance().
                getBufferPool().getNetDirectMemoryUsage();

        RowDataPacket row = new RowDataPacket(TOTAL_FIELD_COUNT);
        long usedforMerge = 0;
        long usedforNetworkd = 0;

        /**
         * the value of -XX:MaxDirectMemorySize
         */
        row.add(StringUtil.encode(JavaUtils.bytesToString2(Platform.getMaxDirectMemory()), c.getCharset().getResults()));

        if (useOffHeapForMerge == 1) {

            /**
             * used DirectMemory for merge
             */
            ConcurrentMap<Long, Long> concurrentHashMap = DbleServer.getInstance().
                    getServerMemory().
                    getResultMergeMemoryManager().getDirectMemorUsage();
            for (Map.Entry<Long, Long> entry : concurrentHashMap.entrySet()) {
                usedforMerge += entry.getValue();
            }
        }

        /**
         * IO packet used in DirectMemory in buffer pool
         */
        for (Map.Entry<Long, Long> entry : networkbufferpool.entrySet()) {
            usedforNetworkd += entry.getValue();
        }

        row.add(StringUtil.encode(JavaUtils.bytesToString2(usedforMerge + usedforNetworkd), c.getCharset().getResults()));


        long totalAvailable = 0;

        if (useOffHeapForMerge == 1) {
            /**
             * when use off-heap , avoid that MaxDirectMemorySize reached the limit of Physical memory.
             * so the valid DirectMemory is MaxDirectMemorySize*DIRECT_SAFETY_FRACTION
             */
            totalAvailable = (long) (Platform.getMaxDirectMemory() * SeverMemory.DIRECT_SAFETY_FRACTION);
        } else {
            totalAvailable = Platform.getMaxDirectMemory();
        }

        row.add(StringUtil.encode(JavaUtils.bytesToString2(totalAvailable - usedforMerge - usedforNetworkd), c.getCharset().getResults()));

        if (useOffHeapForMerge == 1) {
            row.add(StringUtil.encode(("" + SeverMemory.DIRECT_SAFETY_FRACTION), c.getCharset().getResults()));
        } else {
            row.add(StringUtil.encode("1.0", c.getCharset().getResults()));
        }


        long resevedForOs = 0;

        if (useOffHeapForMerge == 1) {
            /**
             * saved for OS
             */
            resevedForOs = (long) ((1 - SeverMemory.DIRECT_SAFETY_FRACTION) *
                    (Platform.getMaxDirectMemory() -
                            2 * DbleServer.getInstance().getTotalNetWorkBufferSize()));
        }

        row.add(StringUtil.encode(resevedForOs > 0 ? JavaUtils.bytesToString2(resevedForOs) : "0", c.getCharset().getResults()));

        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }


}
