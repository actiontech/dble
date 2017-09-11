/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.heartbeat.DBHeartbeat;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;

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

    private static final int FIELD_COUNT = 10;
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

        FIELDS[i] = PacketUtil.getField("RS_CODE", Fields.FIELD_TYPE_LONG);
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

        EOF.setPacketId(++packetId);
    }

    public static void response(ManagerConnection c) {
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
        for (RowDataPacket row : getRows()) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static List<RowDataPacket> getRows() {
        List<RowDataPacket> list = new LinkedList<>();
        ServerConfig conf = DbleServer.getInstance().getConfig();
        // host nodes
        Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
        for (PhysicalDBPool pool : dataHosts.values()) {
            for (PhysicalDatasource ds : pool.getAllDataSources()) {
                DBHeartbeat hb = ds.getHeartbeat();
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(ds.getName().getBytes());
                if (hb != null) {
                    row.add(ds.getConfig().getIp().getBytes());
                    row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
                    row.add(IntegerUtil.toBytes(hb.getStatus()));
                    row.add(IntegerUtil.toBytes(hb.getErrorCount()));
                    row.add(hb.isChecking() ? "checking".getBytes() : "idle".getBytes());
                    row.add(LongUtil.toBytes(hb.getTimeout()));
                    row.add(hb.getRecorder().get().getBytes());
                    String lat = hb.getLastActiveTime();
                    row.add(lat == null ? null : lat.getBytes());
                    row.add(hb.isStop() ? "true".getBytes() : "false".getBytes());
                } else {
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

}
