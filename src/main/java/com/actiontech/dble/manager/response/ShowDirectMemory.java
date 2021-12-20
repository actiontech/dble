/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.singleton.BufferPoolManager;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * show @@directmemory
 */

public final class ShowDirectMemory {
    private ShowDirectMemory() {
    }

    private static final int TOTAL_FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket TOTAL_HEADER = PacketUtil.getHeader(TOTAL_FIELD_COUNT);
    private static final FieldPacket[] TOTAL_FIELDS = new FieldPacket[TOTAL_FIELD_COUNT];
    private static final EOFPacket TOTAL_EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        TOTAL_HEADER.setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_MAXED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_POOL_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i++].setPacketId(++packetId);

        TOTAL_FIELDS[i] = PacketUtil.getField("DIRECT_MEMORY_POOL_USED", Fields.FIELD_TYPE_VAR_STRING);
        TOTAL_FIELDS[i].setPacketId(++packetId);
        TOTAL_EOF.setPacketId(++packetId);


    }


    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = TOTAL_HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : TOTAL_FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = TOTAL_EOF.write(buffer, c, true);
        RowDataPacket row = new RowDataPacket(TOTAL_FIELD_COUNT);
        /* the value of -XX:MaxDirectMemorySize */
        long totalAvailable = Platform.getMaxDirectMemory();
        long poolSize = BufferPoolManager.getBufferPool().capacity();
        long used = poolSize - BufferPoolManager.getBufferPool().size();
        row.add(StringUtil.encode(bytesToString(totalAvailable), c.getCharset().getResults()));
        row.add(StringUtil.encode(bytesToString(poolSize), c.getCharset().getResults()));
        row.add(StringUtil.encode(bytesToString(used), c.getCharset().getResults()));
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


    /*
    convert byte to string present( KB/B)
    this method is not universal. only used for show @@directmemory.
     */
    private static String bytesToString(long size) {
        long cKB = 1L << 10;
        long value = 0;
        String unit = null;

        if (size >= cKB) {
            value = (size / cKB);
            unit = "KB";
        } else {
            value = size;
            unit = "B";
        }

        return value + unit;
    }

}
