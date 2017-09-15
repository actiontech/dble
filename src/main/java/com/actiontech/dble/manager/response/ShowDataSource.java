/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
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
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * ShowDataSource
 *
 * @author mycat
 * @author mycat
 */
public final class ShowDataSource {
    private ShowDataSource() {
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

        FIELDS[i] = PacketUtil.getField("W/R", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("READ_LOAD", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("WRITE_LOAD", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, String name) {
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
        ServerConfig conf = DbleServer.getInstance().getConfig();

        if (null != name) {
            PhysicalDBNode dn = conf.getDataNodes().get(name);
            for (PhysicalDatasource w : dn.getDbPool().getAllDataSources()) {
                RowDataPacket row = getRow(w, c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }

        } else {
            // add all
            for (Map.Entry<String, PhysicalDBPool> entry : conf.getDataHosts().entrySet()) {

                PhysicalDBPool datahost = entry.getValue();

                for (int i = 0; i < datahost.getSources().length; i++) {
                    RowDataPacket row = getRow(datahost.getSources()[i], c.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, c, true);
                    if (datahost.getrReadSources().get(i) != null) {
                        for (PhysicalDatasource w : datahost.getrReadSources().get(i)) {
                            RowDataPacket rsow = getRow(w, c.getCharset().getResults());
                            rsow.setPacketId(++packetId);
                            buffer = rsow.write(buffer, c, true);
                        }
                    }
                }
            }

        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static RowDataPacket getRow(PhysicalDatasource ds,
                                        String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        //row.add(StringUtil.encode(dataNode, charset));
        row.add(StringUtil.encode(ds.getName(), charset));
        row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
        row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
        row.add(StringUtil.encode(ds.isReadNode() ? "R" : "W", charset));
        row.add(IntegerUtil.toBytes(ds.getActiveCount()));
        row.add(IntegerUtil.toBytes(ds.getIdleCount()));
        row.add(IntegerUtil.toBytes(ds.getSize()));
        row.add(LongUtil.toBytes(ds.getExecuteCount()));
        row.add(LongUtil.toBytes(ds.getReadCount()));
        row.add(LongUtil.toBytes(ds.getWriteCount()));
        return row;
    }

}
