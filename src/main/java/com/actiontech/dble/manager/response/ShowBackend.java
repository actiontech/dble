/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;

/**
 * ShowBackend
 *
 * @author mycat
 */
public final class ShowBackend {
    private ShowBackend() {
    }

    private static final int FIELD_COUNT = 18;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("processor",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MYSQLID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LOACL_TCP_PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("ACTIVE_TIME(S)", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CLOSED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        // fields[i] = PacketUtil.getField("run", Fields.FIELD_TYPE_VAR_STRING);
        // fields[i++].packetId = ++packetId;
        FIELDS[i] = PacketUtil.getField("BORROWED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_CLIENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("COLLATION_CONNECTION", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_RESULTS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("TXLEVEL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("AUTOCOMMIT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = HEADER.write(buffer, c, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        buffer = EOF.write(buffer, c, true);
        byte packetId = EOF.getPacketId();
        for (NIOProcessor p : DbleServer.getInstance().getProcessors()) {
            for (BackendConnection bc : p.getBackends().values()) {
                if (bc != null) {
                    RowDataPacket row = getRow(bc, c.getCharset().getResults());
                    if (row != null) {
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, c, true);
                    }
                }
            }
        }
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);
        c.write(buffer);
    }

    private static RowDataPacket getRow(BackendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        if (!(c instanceof MySQLConnection)) {
            return null;
        }
        MySQLConnection conn = (MySQLConnection) c;
        row.add(conn.getProcessor().getName().getBytes());
        row.add(LongUtil.toBytes(c.getId()));
        row.add(LongUtil.toBytes(conn.getThreadId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        row.add(c.isClosed() ? "true".getBytes() : "false".getBytes());
        row.add(c.isBorrowed() ? "true".getBytes() : "false".getBytes());
        row.add(IntegerUtil.toBytes(conn.getWriteQueue().size()));
        row.add(conn.getSchema().getBytes());
        row.add(conn.getCharset().getClient().getBytes());
        row.add(conn.getCharset().getCollation().getBytes());
        row.add(conn.getCharset().getResults().getBytes());
        row.add((conn.getTxIsolation() + "").getBytes());
        row.add((conn.isAutocommit() + "").getBytes());
        return row;
    }
}
