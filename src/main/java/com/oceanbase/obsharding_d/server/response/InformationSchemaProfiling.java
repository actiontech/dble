/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.EOFPacket;
import com.oceanbase.obsharding_d.net.mysql.EOFRowPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.ResultSetHeaderPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;

import java.nio.ByteBuffer;


public final class InformationSchemaProfiling {
    private InformationSchemaProfiling() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    /**
     * response method.
     *
     * @param service
     */
    public static void response(AbstractService service) {


        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("State", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        FIELDS[i + 1] = PacketUtil.getField("Duration", Fields.FIELD_TYPE_DECIMAL);
        FIELDS[i + 1].setPacketId(++packetId);

        FIELDS[i + 2] = PacketUtil.getField("Percentage", Fields.FIELD_TYPE_DECIMAL);
        FIELDS[i + 2].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
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
        packetId = EOF.getPacketId();


        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, service);
    }


}
