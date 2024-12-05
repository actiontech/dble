/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import com.oceanbase.obsharding_d.singleton.FlowController;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlList {
    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_INFO", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("WRITING_QUEUE_BYTES", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("READING_QUEUE_BYTES", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FLOW_CONTROLLED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    private FlowControlList() {

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

        if (FlowController.isEnableFlowControl()) {
            for (RowDataPacket row : getServerConnections(service)) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
            for (RowDataPacket row : getBackendConnections(service)) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getBackendConnections(ManagerService service) {
        List<RowDataPacket> rows = new ArrayList<>();
        IOProcessor[] processors = OBsharding_DServer.getInstance().getBackendProcessors();
        for (IOProcessor p : processors) {
            for (BackendConnection bc : p.getBackends().values()) {
                MySQLResponseService mc = bc.getBackendService();
                if (mc == null) {
                    continue;
                }
                int writeSize = mc.getConnection().getWritingSize().get();
                int readSize = mc.getReadSize();
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("MySQLConnection", service.getCharset().getResults()));
                row.add(LongUtil.toBytes(mc.getConnection().getId()));
                row.add(StringUtil.encode(mc.getConnection().getInstance().getConfig().getUrl() + "/" + mc.getSchema() + " mysqlId = " + mc.getConnection().getThreadId(), service.getCharset().getResults()));
                row.add(LongUtil.toBytes(writeSize));
                row.add(LongUtil.toBytes(readSize));
                row.add(mc.isFlowControlled() ? "true".getBytes() : "false".getBytes());
                rows.add(row);
            }
        }
        return rows;
    }

    private static List<RowDataPacket> getServerConnections(ManagerService service) {
        List<RowDataPacket> rows = new ArrayList<>();
        IOProcessor[] processors = OBsharding_DServer.getInstance().getFrontProcessors();
        for (IOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                AbstractService fcService = fc.getService();
                if (fcService instanceof ShardingService || fcService instanceof RWSplitService) {
                    int size = fc.getWritingSize().get();
                    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode("ServerConnection", service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(fc.getId()));
                    row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort() + "/" + ((FrontendService) fcService).getSchema() + " user = " + ((FrontendService) fcService).getUser().getFullName(), service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(size));
                    row.add(null); // not support
                    row.add(fc.isFrontWriteFlowControlled() ? "true".getBytes() : "false".getBytes());
                    rows.add(row);
                }
            }
        }
        return rows;
    }

}
