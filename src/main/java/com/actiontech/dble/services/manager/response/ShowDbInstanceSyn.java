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
import com.actiontech.dble.statistic.DbInstanceSyncRecorder;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author songwie
 */
public final class ShowDbInstanceSyn {
    private ShowDbInstanceSyn() {
    }

    private static final int FIELD_COUNT = 13;
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

        FIELDS[i] = PacketUtil.getField("SECONDS_BEHIND_MASTER", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SLAVE_IO_RUNNING", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SLAVE_SQL_RUNNING", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SLAVE_IO_STATE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECT_RETRY", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_IO_ERROR", Fields.FIELD_TYPE_VAR_STRING);
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

        for (RowDataPacket row : getRows(service.getCharset().getResults())) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getRows(String charset) {
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
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                if (!states.isEmpty()) {
                    row.add(StringUtil.encode(dbGroupName, charset));
                    row.add(StringUtil.encode(ds.getName(), charset));
                    row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
                    row.add(LongUtil.toBytes(ds.getConfig().getPort()));
                    row.add(StringUtil.encode(states.get("Master_Host"), charset));
                    row.add(LongUtil.toBytes(Long.parseLong(states.get("Master_Port"))));
                    row.add(StringUtil.encode(states.get("Master_User"), charset));
                    String seconds = states.get("Seconds_Behind_Master");
                    row.add(seconds == null ? null : LongUtil.toBytes(Long.parseLong(seconds)));
                    row.add(StringUtil.encode(states.get("Slave_IO_Running"), charset));
                    row.add(StringUtil.encode(states.get("Slave_SQL_Running"), charset));
                    row.add(StringUtil.encode(states.get("Slave_IO_State"), charset));
                    row.add(LongUtil.toBytes(Long.parseLong(states.get("Connect_Retry"))));
                    row.add(StringUtil.encode(states.get("Last_IO_Error"), charset));

                    list.add(row);
                }
            }
        }
        return list;
    }

}
