/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 */
public final class ShowHeartbeat {
    private ShowHeartbeat() {
    }

    private static final int FIELD_COUNT = 11;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RS_CODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RETRY", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TIMEOUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_ACTIVE_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("STOP", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RS_MESSAGE ", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void response(ManagerService service) {
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
        for (RowDataPacket row : getRows()) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getRows() {
        List<RowDataPacket> list = new LinkedList<>();
        ServerConfig conf = DbleServer.getInstance().getConfig();
        // host nodes
        Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
        for (PhysicalDbGroup pool : dbGroups.values()) {
            for (PhysicalDbInstance ds : pool.getDbInstances(true)) {
                MySQLHeartbeat hb = ds.getHeartbeat();
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(ds.getName().getBytes());
                if (hb != null) {
                    row.add(ds.getConfig().getIp().getBytes());
                    row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
                    String code = getRdCode(hb.getStatus());
                    row.add(code == null ? null : code.getBytes());
                    row.add(IntegerUtil.toBytes(hb.getErrorCount()));
                    row.add(hb.isChecking() ? "checking".getBytes() : "idle".getBytes());
                    row.add(LongUtil.toBytes(hb.getHeartbeatTimeout()));
                    row.add(hb.getRecorder().get().getBytes());
                    String lat = hb.getLastActiveTime();
                    row.add(lat == null ? null : lat.getBytes());
                    row.add(hb.isStop() || TimeUtil.currentTimeMillis() < ds.getHeartbeatRecoveryTime() ? "true".getBytes() : "false".getBytes());
                    row.add(hb.getMessage() == null ? null : hb.getMessage().getBytes());
                } else {
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                    row.add(null);
                }
                list.add(row);
            }
        }
        return list;
    }

    public static String getRdCode(int status) {
        String code = null;
        switch (status) {
            case 0:
                code = "init";
                break;
            case 1:
                code = "ok";
                break;
            case -1:
                code = "error";
                break;
            case -2:
                code = "time_out";
                break;
            default:
                break;
        }
        return code;
    }

}
