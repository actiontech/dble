/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SelectVersionComment {
    private SelectVersionComment() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    public static void response(AbstractService service) {

        HEADER.setPacketId(service.nextPacketId());
        FIELDS[0] = PacketUtil.getField("@@VERSION_COMMENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(service.nextPacketId());
        EOF.setPacketId(service.nextPacketId());

        ByteBuffer buffer = service.allocate();

        // writeDirectly header
        buffer = HEADER.write(buffer, service, true);

        // writeDirectly fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // writeDirectly eof
        buffer = EOF.write(buffer, service, true);

        // writeDirectly rows

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(Versions.VERSION_COMMENT);
        row.setPacketId(service.nextPacketId());
        buffer = row.write(buffer, service, true);

        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());

        lastEof.write(buffer, service);
    }


    public static byte setCurrentPacket(AbstractService service) {
        if (service instanceof ShardingService) {
            return (byte) ((ShardingService) service).getSession2().getPacketId().get();
        }
        return 0;
    }

}
