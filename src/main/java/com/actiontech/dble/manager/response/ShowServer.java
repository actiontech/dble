/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;

/**
 * ShowServer
 *
 * @author mycat
 * @author mycat
 */
public final class ShowServer {
    private ShowServer() {
    }

    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("UPTIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USED_MEMORY",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TOTAL_MEMORY",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("MAX_MEMORY",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RELOAD_TIME",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ROLLBACK_TIME",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {
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
        RowDataPacket row = getRow(c.getCharset().getResults());
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String charset) {
        DbleServer server = DbleServer.getInstance();
        long startupTime = server.getStartupTime();
        long now = TimeUtil.currentTimeMillis();
        long uptime = now - startupTime;
        Runtime rt = Runtime.getRuntime();
        long total = rt.totalMemory();
        long max = rt.maxMemory();
        long used = (total - rt.freeMemory());
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(FormatUtil.formatTime(uptime, 3), charset));
        row.add(LongUtil.toBytes(used));
        row.add(LongUtil.toBytes(total));
        row.add(LongUtil.toBytes(max));
        row.add(StringUtil.encode(FormatUtil.formatDate(server.getConfig().getReloadTime()), charset));
        row.add(LongUtil.toBytes(server.getConfig().getRollbackTime()));
        row.add(StringUtil.encode(charset, charset));
        row.add(StringUtil.encode(DbleServer.getInstance().isOnline() ? "ON" : "OFF", charset));
        return row;
    }

}
