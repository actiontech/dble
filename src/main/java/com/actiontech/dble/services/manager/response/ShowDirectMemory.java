/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.memory.unsafe.utils.JavaUtils;
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


    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = TOTAL_HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : TOTAL_FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        // write eof
        buffer = TOTAL_EOF.write(buffer, service, true);
        RowDataPacket row = new RowDataPacket(TOTAL_FIELD_COUNT);
        /* the value of -XX:MaxDirectMemorySize */
        long totalAvailable = Platform.getMaxDirectMemory();
        long poolSize = BufferPoolManager.getBufferPool().capacity();
        long used = poolSize - BufferPoolManager.getBufferPool().size();
        row.add(StringUtil.encode(JavaUtils.bytesToString2(totalAvailable), service.getCharset().getResults()));
        row.add(StringUtil.encode(JavaUtils.bytesToString2(poolSize), service.getCharset().getResults()));
        row.add(StringUtil.encode(JavaUtils.bytesToString2(used), service.getCharset().getResults()));
        // write rows
        byte packetId = TOTAL_EOF.getPacketId();
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

}
