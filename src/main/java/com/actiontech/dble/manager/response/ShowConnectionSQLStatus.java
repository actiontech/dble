/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;

public final class ShowConnectionSQLStatus {
    private ShowConnectionSQLStatus() {
    }
    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("OPERATION", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("START(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("END(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DURATION(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SHARDING_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SQL/REF", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, String id) {
        if (!SlowQueryLog.getInstance().isEnableSlowLog()) {
            c.writeErrMessage(ErrorCode.ER_YES, "please enable @@slow_query_log first");
            return;
        }
        long realId = 0;
        try {
            realId = Long.parseLong(id);
        } catch (NumberFormatException e) {
            c.writeErrMessage(ErrorCode.ER_YES, "front_id must be a number");
            return;
        }
        NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        FrontendConnection target = null;
        for (NIOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null && fc.getId() == realId) {
                    target = fc;
                    break;
                }
            }
        }
        if (target == null) {
            c.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " doesn't exist");
            return;
        }
        if (target instanceof ManagerConnection) {
            c.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " is a manager connection");
            return;
        }
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

        List<String[]> results = ((ServerConnection) target).getSession2().genRunningSQLStage();
        if (results != null) {
            for (String[] result : results) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                for (int i = 0; i < FIELD_COUNT; i++) {
                    row.add(StringUtil.encode(result[i], c.getCharset().getResults()));
                }
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }
}
