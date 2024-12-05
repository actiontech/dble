/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.LongUtil;

import java.nio.ByteBuffer;

public final class SelectSessionTxReadOnly {
    private SelectSessionTxReadOnly() {
    }


    public static void execute(ManagerService service, String column) {
        ByteBuffer buffer = service.allocate();
        byte packetId = 0;
        ResultSetHeaderPacket header = PacketUtil.getHeader(1);
        header.setPacketId(++packetId);
        // write header
        buffer = header.write(buffer, service, true);
        FieldPacket[] fields = new FieldPacket[1];
        fields[0] = PacketUtil.getField(column, Fields.FIELD_TYPE_INT24);
        fields[0].setPacketId(++packetId);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, service, true);
        }
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write eof
        buffer = eof.write(buffer, service, true);

        // write rows
        RowDataPacket row = new RowDataPacket(1);
        row.setPacketId(++packetId);
        row.add(LongUtil.toBytes(service.getUserConfig().isReadOnly() ? 1 : 0));
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

}
