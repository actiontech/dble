/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.parser.ManagerParseHeartbeat;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.statistic.HeartbeatRecorder;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * @author songwie
 */
public final class ShowHeartbeatDetail {
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

    public static void response(ManagerConnection c, String stmt) {


        Pair<String, String> pair = ManagerParseHeartbeat.getPair(stmt);
        String name = pair.getValue();
        if (name.length() == 0) {
            c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
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

        for (RowDataPacket row : getRows(name, c.getCharset().getResults())) {
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

    private static List<RowDataPacket> getRows(String name, String charset) {
        List<RowDataPacket> list = new LinkedList<>();
        ServerConfig conf = DbleServer.getInstance().getConfig();
        String ip = "";
        int port = 0;
        MySQLHeartbeat hb = null;

        Map<String, PhysicalDataHost> dataHosts = conf.getDataHosts();
        for (PhysicalDataHost pool : dataHosts.values()) {
            for (PhysicalDataSource ds : pool.getAllActiveDataSources()) {
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
