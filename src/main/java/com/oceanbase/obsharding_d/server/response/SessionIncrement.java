/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.LongUtil;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SessionIncrement {
    private SessionIncrement() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    public static void response(ShardingService shardingService) {
        HEADER.setPacketId(shardingService.nextPacketId());
        FIELDS[0] = PacketUtil.getField("@@session.auto_increment_increment", Fields.FIELD_TYPE_LONG);
        FIELDS[0].setPacketId(shardingService.nextPacketId());
        EOF.setPacketId(shardingService.nextPacketId());


        ByteBuffer buffer = shardingService.allocate();
        buffer = HEADER.write(buffer, shardingService, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, shardingService, true);
        }
        buffer = EOF.write(buffer, shardingService, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(1));
        row.setPacketId(shardingService.nextPacketId());
        buffer = row.write(buffer, shardingService, true);
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(shardingService.nextPacketId());
        lastEof.write(buffer, shardingService);

    }


    public static byte setCurrentPacket(ShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        return packetId;
    }

}
