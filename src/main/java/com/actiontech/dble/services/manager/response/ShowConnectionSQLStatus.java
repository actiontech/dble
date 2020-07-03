/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;

import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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

        FIELDS[i] = PacketUtil.getField("OPERATION", Fields.FIELD_TYPE_LONG);
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

    public static void execute(ManagerService service, String id) {
        if (!SlowQueryLog.getInstance().isEnableSlowLog()) {
            service.writeErrMessage(ErrorCode.ER_YES, "please enable @@slow_query_log first");
            return;
        }
        long realId = 0;
        try {
            realId = Long.parseLong(id);
        } catch (NumberFormatException e) {
            service.writeErrMessage(ErrorCode.ER_YES, "front_id must be a number");
            return;
        }
        IOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        FrontendConnection target = null;
        for (IOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null && fc.getId() == realId) {
                    target = fc;
                    break;
                }
            }
        }
        if (target == null) {
            service.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " doesn't exist");
            return;
        }
        if (target.isManager()) {
            service.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " is a manager connection");
            return;
        }
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

        List<String[]> results = ((ShardingService) target.getService()).getSession2().genRunningSQLStage();
        if (results != null) {
            for (String[] result : results) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                for (int i = 0; i < FIELD_COUNT; i++) {
                    row.add(StringUtil.encode(result[i], service.getCharset().getResults()));
                }
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);

    }
}
