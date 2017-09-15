/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

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
 * Show Backend Old connection for reload @@config_all
 *
 * @author zhuam
 */
public final class ShowBackendOld {
    private ShowBackendOld() {
    }

    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
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
        FIELDS[i] = PacketUtil.getField("LASTTIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("BORROWED", Fields.FIELD_TYPE_VAR_STRING);
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

        for (BackendConnection bc : NIOProcessor.BACKENDS_OLD) {
            if (bc != null) {
                RowDataPacket row = getRow(bc, c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);
        c.write(buffer);
    }

    private static RowDataPacket getRow(BackendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(c.getId()));
        long threadId = 0;
        if (c instanceof MySQLConnection) {
            threadId = ((MySQLConnection) c).getThreadId();
        }
        row.add(LongUtil.toBytes(threadId));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        row.add(LongUtil.toBytes(c.getLastTime()));
        boolean isBorrowed = c.isBorrowed();
        row.add(isBorrowed ? "true".getBytes() : "false".getBytes());
        return row;
    }

}
