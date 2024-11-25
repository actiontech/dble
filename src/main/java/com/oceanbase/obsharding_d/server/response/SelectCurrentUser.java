/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectCurrentUser implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    public static void response(ShardingService service) {
        if (OBsharding_DServer.getInstance().isOnline()) {
            HEADER.setPacketId(service.nextPacketId());
            FIELDS[0] = PacketUtil.getField("CURRENT_USER()", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[0].setPacketId(service.nextPacketId());
            EOF.setPacketId(service.nextPacketId());

            ByteBuffer buffer = service.allocate();
            buffer = HEADER.write(buffer, service, true);
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, service, true);
            }
            buffer = EOF.write(buffer, service, true);

            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(getUser(service));
            row.setPacketId(service.nextPacketId());
            buffer = row.write(buffer, service, true);
            EOFRowPacket lastEof = new EOFRowPacket();
            lastEof.setPacketId(service.nextPacketId());
            lastEof.write(buffer, service);
        } else {
            ERROR.write(service.getConnection());
        }
    }

    private static byte[] getUser(ShardingService service) {
        return StringUtil.encode(service.getUser().getFullName() + "@%", service.getCharset().getResults());
    }

    public static byte setCurrentPacket(ShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        return packetId;
    }

    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("CURRENT_USER()", Fields.FIELD_TYPE_VAR_STRING));
        return result;
    }

    public List<RowDataPacket> getRows(ShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(getUser(service));
        result.add(row);
        return result;
    }
}
