/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * show front session detail info
 *
 * @author wuzhih
 */
public final class ShowSession {
    private ShowSession() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FRONT_ID", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DN_COUNT", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DN_LIST", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i].setPacketId(++packetId);
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
        for (IOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {
                if (front.isManager()) {
                    continue;
                }
                RowDataPacket row = getRow((ShardingService) front.getService(), service.getCharset().getResults());
                if (row != null) {
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(ShardingService sc, String charset) {
        StringBuilder sb = new StringBuilder();
        NonBlockingSession session = sc.getSession2();
        Collection<BackendConnection> backConnections = session.getTargetMap().values();
        int cnCount = backConnections.size();
        if (cnCount == 0) {
            return null;
        }
        for (BackendConnection backCon : backConnections) {
            sb.append(backCon).append("\r\n");
        }
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(sc.getConnection().getId() + "", charset));
        row.add(StringUtil.encode(cnCount + "", charset));
        row.add(StringUtil.encode(sb.toString(), charset));
        return row;
    }
}
