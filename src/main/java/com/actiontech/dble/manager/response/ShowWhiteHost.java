/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class ShowWhiteHost {
    private ShowWhiteHost() {
    }

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("IP", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);


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

        Map<String, List<UserConfig>> map = DbleServer.getInstance().getConfig().getFirewall().getWhitehost();
        for (Map.Entry<String, List<UserConfig>> entry : map.entrySet()) {
            List<UserConfig> userConfigs = entry.getValue();
            StringBuilder users = new StringBuilder();
            for (int i = 0; i < userConfigs.size(); i++) {
                if (i > 0) {
                    users.append(",");
                }
                users.append(userConfigs.get(i).getName());
            }
            RowDataPacket row = getRow(entry.getKey(), users.toString(), c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String ip, String user, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(ip, charset));
        row.add(StringUtil.encode(user, charset));
        return row;
    }


}
