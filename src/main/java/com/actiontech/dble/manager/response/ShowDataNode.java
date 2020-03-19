/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.PairUtil;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * ShowDataNode
 *
 * @author mycat
 * @author mycat
 */
public final class ShowDataNode {
    private static final NumberFormat NF = DecimalFormat.getInstance();
    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        NF.setMaximumFractionDigits(3);

        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DATHOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SCHEMA_EXISTS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RECOVERY_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowDataNode() {
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
        Map<String, PhysicalDataNode> dataNodes = conf.getDataNodes();
        List<String> keys = new ArrayList<>();
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
            RowDataPacket row = getRow(dataNodes.get(key), c.getCharset().getResults());
            if (row != null) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static RowDataPacket getRow(PhysicalDataNode node, String charset) {
        PhysicalDataHost pool = node.getDataHost();
        PhysicalDataSource ds = pool.getWriteSource();
        if (ds != null) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(node.getName(), charset));
            row.add(StringUtil.encode(
                    node.getDataHost().getHostName() + '/' + node.getDatabase(),
                    charset));
            row.add(StringUtil.encode(node.isSchemaExists() ? "true" : "false", charset));
            int active = ds.getActiveCountForSchema(node.getDatabase());
            int idle = ds.getIdleCountForSchema(node.getDatabase());
            row.add(IntegerUtil.toBytes(active));
            row.add(IntegerUtil.toBytes(idle));
            row.add(IntegerUtil.toBytes(ds.getSize()));
            row.add(LongUtil.toBytes(ds.getExecuteCountForSchema(node.getDatabase())));
            long recoveryTime = pool.getWriteSource().getHeartbeatRecoveryTime() - TimeUtil.currentTimeMillis();
            row.add(LongUtil.toBytes(recoveryTime > 0 ? recoveryTime / 1000L : -1L));
            return row;
        } else {
            return null;
        }
    }


}
