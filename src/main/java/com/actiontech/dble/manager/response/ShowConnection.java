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
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;

/**
 * Show Active Connection
 *
 * @author mycat
 * @author mycat
 */
public final class ShowConnection {
    private ShowConnection() {
    }

    private static final int FIELD_COUNT = 17;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PROCESSOR",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_CLIENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("COLLATION_CONNECTION", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_RESULTS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ALIVE_TIME(S)",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RECV_BUFFER", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TXLEVEL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("AUTOCOMMIT",
                Fields.FIELD_TYPE_VAR_STRING);
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
        NIOProcessor[] processors = DbleServer.getInstance().getProcessors();
        for (NIOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null) {
                    RowDataPacket row = getRow(fc, c.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, c, true);
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(FrontendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(c.getProcessor().getName().getBytes());
        row.add(LongUtil.toBytes(c.getId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(StringUtil.encode(c.getUser(), charset));
        row.add(StringUtil.encode(c.getSchema(), charset));
        row.add(StringUtil.encode(c.getCharset().getClient(), charset));
        row.add(StringUtil.encode(c.getCharset().getCollation(), charset));
        row.add(StringUtil.encode(c.getCharset().getResults(), charset));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        ByteBuffer bb = c.getReadBuffer();
        row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
        row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));

        String txLevel = "";
        String txAutommit = "";
        if (c instanceof ServerConnection) {
            ServerConnection mysqlC = (ServerConnection) c;
            txLevel = mysqlC.getTxIsolation() + "";
            txAutommit = mysqlC.isAutocommit() + "";
        }
        row.add(txLevel.getBytes());
        row.add(txAutommit.getBytes());

        return row;
    }

}
