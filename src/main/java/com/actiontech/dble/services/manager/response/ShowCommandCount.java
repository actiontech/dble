/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;

/**
 * Show Command times
 *
 * @author mycat
 * @author mycat
 */
public final class ShowCommandCount {
    private ShowCommandCount() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[0] = PacketUtil.getField("QUERY_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        byte packetId = EOF.getPacketId();
        RowDataPacket row = getRow();
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow() {
        long querys = 0;
        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            querys += p.getCommands().queryCount();
        }

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(querys));
        return row;
    }

}
