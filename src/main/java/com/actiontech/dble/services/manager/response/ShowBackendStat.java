/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * ShowBackend
 *
 * @author mycat
 */
public final class ShowBackendStat {
    private ShowBackendStat() {
    }

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TOTAL", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        buffer = HEADER.write(buffer, service, true);

        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        buffer = EOF.write(buffer, service, true);

        HashMap<String, BackendStat> infos = stat();

        byte packetId = EOF.getPacketId();
        for (Map.Entry<String, BackendStat> entry : infos.entrySet()) {
            RowDataPacket row = getRow(entry.getValue(), service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(BackendStat info, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(info.getHost(), charset));
        row.add(LongUtil.toBytes(info.getPort()));
        row.add(LongUtil.toBytes(info.getActive()));
        row.add(LongUtil.toBytes(info.getTotal()));
        return row;
    }

    private static HashMap<String, BackendStat> stat() {
        HashMap<String, BackendStat> all = new HashMap<>();

        for (IOProcessor p : DbleServer.getInstance().getBackendProcessors()) {
            for (BackendConnection bc : p.getBackends().values()) {
                String host = bc.getHost();
                long port = bc.getPort();
                BackendStat info = all.get(host + port);
                if (info == null) {
                    info = new BackendStat(host, port);
                    all.put(host + port, info);
                }

                if (bc.getState() == PooledConnection.STATE_IN_USE) {
                    info.addActive();
                }
                info.addTotal();
            }
        }
        return all;
    }

    private static class BackendStat {
        private String host;
        private long port;
        private long active;
        private long total;

        BackendStat(String host, long port) {
            this.host = host;
            this.port = port;
            this.active = 0;
            this.total = 0;
        }

        public String getHost() {
            return this.host;
        }

        public long getPort() {
            return this.port;
        }

        public void addActive() {
            this.active++;
        }

        public void addTotal() {
            this.total++;
        }

        public long getActive() {
            return this.active;
        }

        public long getTotal() {
            return this.total;
        }
    }
}
