/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.heartbeat.DBHeartbeat;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.statistic.DataSourceSyncRecorder;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author songwie
 */
public final class ShowDatasourceCluster {
    private ShowDatasourceCluster() {
    }

    private static final int FIELD_COUNT = 17;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    /*private static final String[] MYSQL_CLUSTER_STAUTS_COLMS = new String[] {
    "wsrep_incoming_addresses","wsrep_cluster_size","wsrep_cluster_status", "wsrep_connected", "wsrep_flow_control_paused",
    "wsrep_local_state_comment","wsrep_ready","wsrep_flow_control_paused_ns","wsrep_flow_control_recv","wsrep_local_bf_aborts",
    "wsrep_local_recv_queue_avg","wsrep_local_send_queue_avg","wsrep_apply_oool","wsrep_apply_oooe"};*/


    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("host", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("port", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_incoming_addresses", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_cluster_size", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_cluster_status", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_connected", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_flow_control_paused", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_local_state_comment", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_ready", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_flow_control_paused_ns", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_flow_control_recv", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_local_bf_aborts", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_local_recv_queue_avg", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_local_send_queue_avg", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_apply_oool", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("wsrep_apply_oooe", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void response(ManagerConnection c) {
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

        for (RowDataPacket row : getRows(c.getCharset().getResults())) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static List<RowDataPacket> getRows(String charset) {
        List<RowDataPacket> list = new LinkedList<>();
        ServerConfig conf = DbleServer.getInstance().getConfig();
        // host nodes
        Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
        for (PhysicalDBPool pool : dataHosts.values()) {
            for (PhysicalDatasource ds : pool.getAllDataSources()) {
                DBHeartbeat hb = ds.getHeartbeat();
                DataSourceSyncRecorder record = hb.getAsynRecorder();
                Map<String, String> states = record.getRecords();
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                if (!states.isEmpty()) {
                    row.add(StringUtil.encode(ds.getName(), charset));
                    row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
                    row.add(LongUtil.toBytes(ds.getConfig().getPort()));

                    row.add(StringUtil.encode(states.get("wsrep_incoming_addresses") == null ? "" : states.get("wsrep_incoming_addresses"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_cluster_size") == null ? "" : states.get("wsrep_cluster_size"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_cluster_status") == null ? "" : states.get("wsrep_cluster_status"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_connected") == null ? "" : states.get("wsrep_connected"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_flow_control_paused") == null ? "" : states.get("wsrep_flow_control_paused"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_local_state_comment") == null ? "" : states.get("wsrep_local_state_comment"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_ready") == null ? "" : states.get("wsrep_ready"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_flow_control_paused_ns") == null ? "" : states.get("wsrep_flow_control_paused_ns"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_flow_control_recv") == null ? "" : states.get("wsrep_flow_control_recv"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_local_bf_aborts") == null ? "" : states.get("wsrep_local_bf_aborts"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_local_recv_queue_avg") == null ? "" : states.get("wsrep_local_recv_queue_avg"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_local_send_queue_avg") == null ? "" : states.get("wsrep_local_recv_queue_avg"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_apply_oool") == null ? "" : states.get("wsrep_apply_oool"), charset));
                    row.add(StringUtil.encode(states.get("wsrep_apply_oooe") == null ? "" : states.get("wsrep_apply_oooe"), charset));


                    list.add(row);
                }
            }
        }
        return list;
    }

}
