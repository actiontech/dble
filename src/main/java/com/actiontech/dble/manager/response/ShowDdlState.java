/*
 * Copyright (C) 2016-2019 ActionTech.
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
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

public final class ShowDdlState {
    private ShowDdlState() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Schema", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Table", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Sql", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

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

        Map<String, String> lockTables = DbleServer.getInstance().getTmManager().getLockTables();
        for (Map.Entry<String, String> entry : lockTables.entrySet()) {
            String[] infos = entry.getKey().split("\\.");
            if (infos.length == 2) {
                RowDataPacket row = getRow(infos[0], infos[1], entry.getValue(), c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }

    private static RowDataPacket getRow(String schema, String table, String sql, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode(table, charset));
        row.add(StringUtil.encode(sql, charset));
        return row;
    }
}
