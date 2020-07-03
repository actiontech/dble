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

/**
 * @author mycat
 */
public final class SelectIdentity {
    private SelectIdentity() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);


    public static void response(ShardingService service, String stmt, int aliasIndex, final String orgName) {
        String alias = ParseUtil.parseAlias(stmt, aliasIndex);
        if (alias == null) {
            alias = orgName;
        }

        ByteBuffer buffer = service.allocate();

        HEADER.setPacketId(service.nextPacketId());
        // writeDirectly header
        buffer = HEADER.write(buffer, service, true);

        // writeDirectly fields

        FieldPacket field = PacketUtil.getField(alias, orgName, Fields.FIELD_TYPE_LONGLONG);
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


}
