/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.memory.unsafe.utils.JavaUtils;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * show @@directmemory
 *
 */

public final class ShowDirectMemory {
    private ShowDirectMemory() {
    }

    private static final int DETAIL_FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket DETAIL_HEADER = PacketUtil.getHeader(DETAIL_FIELD_COUNT);
    private static final FieldPacket[] DETAIL_FIELDS = new FieldPacket[DETAIL_FIELD_COUNT];
    private static final EOFPacket DETAIL_EOF = new EOFPacket();


    private static final int TOTAL_FIELD_COUNT = 3;
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
        DETAIL_FIELDS[i].setPacketId(++packetId);
        DETAIL_EOF.setPacketId(++packetId);


        i = 0;
        packetId = 0;

        TOTAL_HEADER.setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_MAXED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_USED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_AVAILABLE", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i].setPacketId(++packetId);
        TOTAL_EOF.setPacketId(++packetId);


    }


    public static void execute(ManagerConnection c, int showType) {

        if (showType == 1) {
            showDirectMemoryTotal(c);
        } else if (showType == 2) {
            showDirectMemoryDetail(c);
        }
    }


    private static void showDirectMemoryDetail(ManagerConnection c) {

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

        ConcurrentMap<Long, Long> networkBufferPool = DbleServer.getInstance().
                getBufferPool().getNetDirectMemoryUsage();

        for (Map.Entry<Long, Long> entry : networkBufferPool.entrySet()) {
            RowDataPacket row = new RowDataPacket(DETAIL_FIELD_COUNT);
            long value = entry.getValue();
            row.add(StringUtil.encode(String.valueOf(entry.getKey()), c.getCharset().getResults()));
            /* DIRECT_MEMORY belong to Buffer Pool */
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


    private static void showDirectMemoryTotal(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = TOTAL_HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : TOTAL_FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = TOTAL_EOF.write(buffer, c, true);

        ConcurrentMap<Long, Long> networkBufferPool = DbleServer.getInstance().
                getBufferPool().getNetDirectMemoryUsage();

        RowDataPacket row = new RowDataPacket(TOTAL_FIELD_COUNT);
        long usedForNetwork = 0;

        /* the value of -XX:MaxDirectMemorySize */
        long totalAvailable = Platform.getMaxDirectMemory();
        row.add(StringUtil.encode(JavaUtils.bytesToString2(totalAvailable), c.getCharset().getResults()));
        /* IO packet used in DirectMemory in buffer pool */
        for (Map.Entry<Long, Long> entry : networkBufferPool.entrySet()) {
            usedForNetwork += entry.getValue();
        }
        row.add(StringUtil.encode(JavaUtils.bytesToString2(usedForNetwork), c.getCharset().getResults()));
        row.add(StringUtil.encode(JavaUtils.bytesToString2(totalAvailable - usedForNetwork), c.getCharset().getResults()));

        // write rows
        byte packetId = TOTAL_EOF.getPacketId();
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
