/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author mycat
 */
public final class ShowDatabases {
    private ShowDatabases() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void response(ServerConnection c) {
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
        Map<String, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(c.getUser());
        if (user != null) {
            TreeSet<String> schemaSet = new TreeSet<>();
            Set<String> schemaList = user.getSchemas();
            if (schemaList == null || schemaList.size() == 0) {
                schemaSet.addAll(conf.getSchemas().keySet());
            } else {
                for (String schema : schemaList) {
                    schemaSet.add(schema);
                }
            }
            for (String name : schemaSet) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(name, c.getCharset().getResults()));
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

}
