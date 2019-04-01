/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.xa.XASessionCheck;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * show front session and associated xa transaction details
 *
 * @author collapsar
 */
public final class ShowXASession {
    private ShowXASession() {
    }

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SESSION", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("XA_ID", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("XA_STATE", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DATANODES", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i].setPacketId(++packetId);

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
        final XASessionCheck xaCheck = DbleServer.getInstance().getXaSessionCheck();
        for (NonBlockingSession commitSession : xaCheck.getCommittingSession().values()) {
            RowDataPacket row = getRow(commitSession, c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }
        for (NonBlockingSession rollbackSession : xaCheck.getRollbackingSession().values()) {
            RowDataPacket row = getRow(rollbackSession, c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(NonBlockingSession session, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(session.getSource().getId() + "", charset));
        row.add(StringUtil.encode(session.getSessionXaID() + "", charset));
        row.add(StringUtil.encode(session.getXaState().getState(), charset));
        StringBuilder sb = new StringBuilder();
        for (RouteResultsetNode node : session.getTargetKeys()) {
            sb.append(node.getName() + " ");
        }
        row.add(StringUtil.encode(sb.toString(), charset));
        return row;
    }
}
