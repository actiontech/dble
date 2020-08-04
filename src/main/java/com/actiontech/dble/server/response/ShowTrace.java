/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;

public final class ShowTrace {
    private ShowTrace() {
    }

    private static final int FIELD_COUNT = 6;
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    static {
        FIELDS[0] = PacketUtil.getField("OPERATION", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[1] = PacketUtil.getField("START(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[2] = PacketUtil.getField("END(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[3] = PacketUtil.getField("DURATION(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[4] = PacketUtil.getField("SHARDING_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[5] = PacketUtil.getField("SQL/REF", Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void response(ShardingService shardingService) {
        ByteBuffer buffer = shardingService.allocate();

        // writeDirectly header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        byte packetId = header.getPacketId();
        buffer = header.write(buffer, shardingService, true);

        // writeDirectly fields
        for (FieldPacket field : FIELDS) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, shardingService, true);
        }

        // writeDirectly eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, shardingService, true);

        List<String[]> results = shardingService.getSession2().genTraceResult();
        if (results != null) {
            for (String[] result : results) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                for (int i = 0; i < FIELD_COUNT; i++) {
                    row.add(StringUtil.encode(result[i], shardingService.getCharset().getResults()));
                }
                row.setPacketId(++packetId);
                buffer = row.write(buffer, shardingService, true);
            }
        }
        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, shardingService);
    }
}
