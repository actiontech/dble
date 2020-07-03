/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectLastInsertId implements InnerFuncResponse {
    private static final String ORG_NAME = "LAST_INSERT_ID()";
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    public static void response(ShardingService service, String stmt, int aliasIndex) {
        String alias = ParseUtil.parseAlias(stmt, aliasIndex);
        if (alias == null) {
            alias = ORG_NAME;
        }

        ByteBuffer buffer = service.allocate();

        HEADER.setPacketId(service.nextPacketId());
        // writeDirectly header
        buffer = HEADER.write(buffer, service, true);

        // writeDirectly fields

        FieldPacket field = PacketUtil.getField(alias, ORG_NAME, Fields.FIELD_TYPE_LONGLONG);
        field.setPacketId(service.nextPacketId());
        buffer = field.write(buffer, service, true);

        // writeDirectly eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(service.nextPacketId());
        buffer = eof.write(buffer, service, true);

        // writeDirectly rows
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getLastInsertId()));
        row.setPacketId(service.nextPacketId());
        buffer = row.write(buffer, service, true);

        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());
        lastEof.write(buffer, service);
    }


    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField(ORG_NAME, Fields.FIELD_TYPE_LONGLONG));
        return result;
    }

    public List<RowDataPacket> getRows(ShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getLastInsertId()));
        result.add(row);
        return result;
    }
}
