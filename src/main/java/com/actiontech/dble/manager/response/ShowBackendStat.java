/*
* Copyright (C) 2016-2019 ActionTech.
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
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;

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
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        buffer = HEADER.write(buffer, c, true);

        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        buffer = EOF.write(buffer, c, true);

        HashMap<String, BackendStat> infos = stat();

        byte packetId = EOF.getPacketId();
        for (Map.Entry<String, BackendStat> entry: infos.entrySet()) {
            RowDataPacket row = getRow(entry.getValue(), c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);
        c.write(buffer);
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
        HashMap<String, BackendStat> all = new HashMap<String, BackendStat>();

        for (NIOProcessor p : DbleServer.getInstance().getBackendProcessors()) {
            for (BackendConnection bc : p.getBackends().values()) {
                if ((bc == null) || !(bc instanceof MySQLConnection)) {
                    break;
                }

                MySQLConnection con = (MySQLConnection) bc;
                String host = con.getHost();
                long port = con.getPort();
                BackendStat info = all.get(host + Long.toString(port));
                if (info == null) {
                    info = new BackendStat(host, port);
                    all.put(host + Long.toString(port), info);
                }

                if (con.isBorrowed()) {
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
