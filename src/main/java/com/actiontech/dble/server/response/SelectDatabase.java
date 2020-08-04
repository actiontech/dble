/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectDatabase implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    public static void response(ShardingService shardingService) {

        HEADER.setPacketId(shardingService.nextPacketId());
        FIELDS[0] = PacketUtil.getField("DATABASE()", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(shardingService.nextPacketId());
        EOF.setPacketId(shardingService.nextPacketId());
        ByteBuffer buffer = shardingService.allocate();
        buffer = HEADER.write(buffer, shardingService, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, shardingService, true);
        }
        buffer = EOF.write(buffer, shardingService, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(shardingService.getSchema(), shardingService.getCharset().getResults()));
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

    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("DATABASE()", Fields.FIELD_TYPE_VAR_STRING));
        return result;
    }

    public List<RowDataPacket> getRows(ShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(service.getSchema(), service.getCharset().getResults()));
        result.add(row);
        return result;
    }
}
