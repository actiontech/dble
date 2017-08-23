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
import io.mycat.config.model.SchemaConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.parser.util.Pair;
import io.mycat.route.parser.util.PairUtil;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * 查看数据节点信息
 *
 * @author mycat
 * @author mycat
 */
public final class ShowDataNode {

    private static final NumberFormat NF = DecimalFormat.getInstance();
    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil
            .getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        NF.setMaximumFractionDigits(3);

        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil
                .getField("DATHOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("INDEX", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("RECOVERY_TIME",
                Fields.FIELD_TYPE_LONGLONG);
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
        Map<String, PhysicalDBNode> dataNodes = conf.getDataNodes();
        List<String> keys = new ArrayList<String>();
        if (StringUtil.isEmpty(name)) {
            keys.addAll(dataNodes.keySet());
        } else {
            SchemaConfig sc = conf.getSchemas().get(name);
            if (null != sc) {
                keys.addAll(sc.getAllDataNodes());
            }
        }
        Collections.sort(keys, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                Pair<String, Integer> p1 = PairUtil.splitIndex(o1, '[', ']');
                Pair<String, Integer> p2 = PairUtil.splitIndex(o2, '[', ']');
                if (p1.getKey().compareTo(p2.getKey()) == 0) {
                    return p1.getValue() - p2.getValue();
                } else {
                    return p1.getKey().compareTo(p2.getKey());
                }
            }
        });
        for (String key : keys) {
            RowDataPacket row = getRow(dataNodes.get(key), c.getCharset());
            if (row != null) {
                row.packetId = ++packetId;
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static RowDataPacket getRow(PhysicalDBNode node, String charset) {
        PhysicalDBPool pool = node.getDbPool();
        PhysicalDatasource ds = pool.getSource();
        if (ds != null) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(node.getName(), charset));
            row.add(StringUtil.encode(
                    node.getDbPool().getHostName() + '/' + node.getDatabase(),
                    charset));
            int active = ds.getActiveCountForSchema(node.getDatabase());
            int idle = ds.getIdleCountForSchema(node.getDatabase());
            row.add(IntegerUtil.toBytes(pool.getActiveIndex()));
            row.add(IntegerUtil.toBytes(active));
            row.add(IntegerUtil.toBytes(idle));
            row.add(IntegerUtil.toBytes(ds.getSize()));
            row.add(LongUtil.toBytes(ds.getExecuteCountForSchema(node.getDatabase())));
            long recoveryTime = pool.getSource().getHeartbeatRecoveryTime()
                    - TimeUtil.currentTimeMillis();
            row.add(LongUtil.toBytes(recoveryTime > 0 ? recoveryTime / 1000L : -1L));
            return row;
        } else {
            return null;
        }
    }


}
