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
import com.actiontech.dble.route.parser.ManagerParseShow;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.DbInstanceSyncRecorder;
import com.actiontech.dble.statistic.DbInstanceSyncRecorder.Record;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author songwie
 */
public final class ShowDbInstanceSynDetail {
    private ShowDbInstanceSynDetail() {
    }

    private static final int FIELD_COUNT = 9;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DB_GROUP", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("MASTER_HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("MASTER_PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("MASTER_USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DATETIME);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SECONDS_BEHIND_MASTER", Fields.FIELD_TYPE_LONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void response(ManagerService service, String stmt) {
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

        String name = ManagerParseShow.getWhereParameter(stmt);
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
        // host nodes
        Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
        for (Map.Entry<String, PhysicalDbGroup> entry : dbGroups.entrySet()) {
            String dbGroupName = entry.getKey();
            PhysicalDbGroup pool = entry.getValue();
            for (PhysicalDbInstance ds : pool.getDbInstances(false)) {
                if (ds.isDisabled()) {
                    continue;
                }
                MySQLHeartbeat hb = ds.getHeartbeat();
                DbInstanceSyncRecorder record = hb.getAsyncRecorder();
                Map<String, String> states = record.getRecords();
                if (name.equals(ds.getName())) {
                    List<Record> data = record.getAsyncRecords();
                    for (Record r : data) {
                        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                        row.add(StringUtil.encode(dbGroupName, charset));
                        row.add(StringUtil.encode(ds.getName(), charset));
                        row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
                        row.add(LongUtil.toBytes(ds.getConfig().getPort()));
                        row.add(StringUtil.encode(states.get("Master_Host"), charset));
                        row.add(LongUtil.toBytes(Long.parseLong(states.get("Master_Port"))));
                        row.add(StringUtil.encode(states.get("Master_User"), charset));
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String time = sdf.format(new Date(r.getTime()));
                        row.add(StringUtil.encode(time, charset));
                        row.add(LongUtil.toBytes((Long) r.getValue()));

                        list.add(row);
                    }
                    break;
                }

            }
        }
        return list;
    }
}
