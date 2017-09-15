/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
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

        FIELDS[i] = PacketUtil.getField("SESSION", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DN_COUNT", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DN_LIST", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);

        // write rows
        byte packetId = EOF.getPacketId();
        for (NIOProcessor process : DbleServer.getInstance().getProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {

                if (!(front instanceof ServerConnection)) {
                    continue;
                }
                ServerConnection sc = (ServerConnection) front;
                RowDataPacket row = getRow(sc, c.getCharset().getResults());
                if (row != null) {
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, c, true);
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(ServerConnection sc, String charset) {
        StringBuilder sb = new StringBuilder();
        NonBlockingSession ssesion = sc.getSession2();
        Collection<BackendConnection> backConnections = ssesion.getTargetMap().values();
        int cncount = backConnections.size();
        if (cncount == 0) {
            return null;
        }
        for (BackendConnection backCon : backConnections) {
            sb.append(backCon).append("\r\n");
        }
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(sc.getId() + "", charset));
        row.add(StringUtil.encode(cncount + "", charset));
        row.add(StringUtil.encode(sb.toString(), charset));
        return row;
    }
}
