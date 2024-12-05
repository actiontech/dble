/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.AnalysisUserConfig;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.server.status.SlowQueryLog;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import com.oceanbase.obsharding_d.util.StringUtil;

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
        IOProcessor[] processors = OBsharding_DServer.getInstance().getFrontProcessors();
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
        AbstractService frontService = target.getService();
        if (target.isManager()) {
            service.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " is a manager connection");
            return;
        }
        if (frontService instanceof RWSplitService) {
            //temporary process
            if (((RWSplitService) frontService).getUserConfig() instanceof AnalysisUserConfig) {
                service.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " is a analysis connection");
                return;
            }
            service.writeErrMessage(ErrorCode.ER_YES, "The front_id " + id + " is a RWSplit connection");
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

        List<String[]> results = ((ShardingService) frontService).getSession2().genRunningSQLStage();
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
