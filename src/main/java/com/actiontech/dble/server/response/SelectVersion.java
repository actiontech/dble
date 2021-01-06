/*
* Copyright (C) 2016-2021 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectVersion implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    public static void response(ShardingService service) {
        HEADER.setPacketId(service.nextPacketId());
        FIELDS[0] = PacketUtil.getField("VERSION()", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(service.nextPacketId());
        EOF.setPacketId(service.nextPacketId());


        ByteBuffer buffer = service.allocate();
        buffer = HEADER.write(buffer, service, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        buffer = EOF.write(buffer, service, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(Versions.getServerVersion());
        row.setPacketId(service.nextPacketId());
        buffer = row.write(buffer, service, true);
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());
        lastEof.write(buffer, service);
    }


    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("VERSION()", Fields.FIELD_TYPE_VAR_STRING));
        return result;
    }

    public List<RowDataPacket> getRows(ShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(Versions.getServerVersion());
        result.add(row);
        return result;
    }

}
