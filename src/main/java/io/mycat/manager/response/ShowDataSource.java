/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 查看数据源信息
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
        HEADER.packetId = ++packetId;

        /*fields[i] = PacketUtil.getField("DATANODE",
                Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;*/

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("W/R", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("READ_LOAD", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("WRITE_LOAD", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        EOF.packetId = ++packetId;
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
        byte packetId = EOF.packetId;
        MycatConfig conf = MycatServer.getInstance().getConfig();

        if (null != name) {
            PhysicalDBNode dn = conf.getDataNodes().get(name);
            for (PhysicalDatasource w : dn.getDbPool().getAllDataSources()) {
                RowDataPacket row = getRow(w, c.getCharset());
                row.packetId = ++packetId;
                buffer = row.write(buffer, c, true);
            }

        } else {
            // add all
            for (Map.Entry<String, PhysicalDBPool> entry : conf.getDataHosts().entrySet()) {

                PhysicalDBPool datahost = entry.getValue();

                for (int i = 0; i < datahost.getSources().length; i++) {
                    RowDataPacket row = getRow(datahost.getSources()[i], c.getCharset());
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c, true);
                    if (datahost.getrReadSources().get(i) != null) {
                        for (PhysicalDatasource w : datahost.getrReadSources().get(i)) {
                            RowDataPacket rsow = getRow(w, c.getCharset());
                            rsow.packetId = ++packetId;
                            buffer = rsow.write(buffer, c, true);
                        }
                    }
                }
            }

        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
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
