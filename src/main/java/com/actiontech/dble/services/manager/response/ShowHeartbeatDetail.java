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
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.HeartbeatRecorder;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author songwie
 */
public final class ShowHeartbeatDetail {
    public static final Pattern HEARTBEAT_DETAIL_STMT = Pattern.compile("show\\s+@@heartbeat.detail\\s+where\\s+(name)\\s*=\\s*([^\\s]+)\\s*(;)?", Pattern.CASE_INSENSITIVE);

    private ShowHeartbeatDetail() {
    }

    private static final int FIELD_COUNT = 5;
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

        FIELDS[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DATETIME);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void response(ManagerService service, String stmt) {
        Matcher matcher = HEARTBEAT_DETAIL_STMT.matcher(stmt);
        String name;
        if (!matcher.matches() || (name = matcher.group(2)) == null) {
            service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            return;
        }
        name = StringUtil.removeAllApostrophe(name);
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

        for (RowDataPacket row : getRows(name, service.getCharset().getResults())) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getRows(String name, String charset) {
        List<RowDataPacket> list = new LinkedList<>();
        ServerConfig conf = DbleServer.getInstance().getConfig();
        String ip = "";
        int port = 0;
        MySQLHeartbeat hb = null;

        Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
        for (PhysicalDbGroup pool : dbGroups.values()) {
            for (PhysicalDbInstance ds : pool.getDbInstances(false)) {
                if (name.equals(ds.getName())) {
                    hb = ds.getHeartbeat();
                    ip = ds.getConfig().getIp();
                    port = ds.getConfig().getPort();
                    break;
                }
            }
        }
        if (hb != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Queue<HeartbeatRecorder.Record> heartbeatRecorders = hb.getRecorder().getRecordsAll();
            for (HeartbeatRecorder.Record record : heartbeatRecorders) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(name, charset));
                row.add(StringUtil.encode(ip, charset));
                row.add(IntegerUtil.toBytes(port));
                long time = record.getTime();
                String timeStr = sdf.format(new Date(time));
                row.add(StringUtil.encode(timeStr, charset));
                row.add(LongUtil.toBytes(record.getValue()));

                list.add(row);
            }
        } else {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(null);
            row.add(null);
            row.add(null);
            row.add(null);
            row.add(null);
            list.add(row);
        }

        return list;
    }

}
