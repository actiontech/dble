/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.route.parser.util.PairUtil;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.IntegerUtil;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.oceanbase.obsharding_d.util.TimeUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * ShowShardingNode
 *
 * @author mycat
 * @author mycat
 */
public final class ShowShardingNode {
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

        FIELDS[i] = PacketUtil.getField("DB_GROUP", Fields.FIELD_TYPE_VAR_STRING);
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

    private ShowShardingNode() {
    }

    public static void execute(ManagerService service, String name) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        byte packetId = EOF.getPacketId();
        ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
        Map<String, ShardingNode> shardingNodes = conf.getShardingNodes();
        List<String> keys = new ArrayList<>();
        if (StringUtil.isEmpty(name)) {
            keys.addAll(shardingNodes.keySet());
        } else {
            SchemaConfig sc = conf.getSchemas().get(name);
            if (null != sc) {
                keys.addAll(sc.getAllShardingNodes());
            }
        }
        keys.sort(new Comparator<String>() {
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
            RowDataPacket row = getRow(shardingNodes.get(key), service.getCharset().getResults());
            if (row != null) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(ShardingNode node, String charset) {
        PhysicalDbGroup pool = node.getDbGroup();
        PhysicalDbInstance ds = pool.getWriteDbInstance();
        if (ds != null) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(node.getName(), charset));
            row.add(StringUtil.encode(
                    node.getDbGroup().getGroupName() + '/' + node.getDatabase(),
                    charset));
            row.add(StringUtil.encode(node.isSchemaExists() ? "true" : "false", charset));
            int active = ds.getActiveConnections(node.getDatabase());
            int idle = ds.getIdleConnections(node.getDatabase());
            row.add(IntegerUtil.toBytes(active));
            row.add(IntegerUtil.toBytes(idle));
            row.add(IntegerUtil.toBytes(ds.getConfig().getMaxCon()));
            row.add(LongUtil.toBytes(0));
            long recoveryTime = ds.getHeartbeat().getHeartbeatRecoveryTime() - TimeUtil.currentTimeMillis();
            row.add(LongUtil.toBytes(recoveryTime > 0 ? recoveryTime / 1000L : -1L));
            return row;
        } else {
            return null;
        }
    }


}
