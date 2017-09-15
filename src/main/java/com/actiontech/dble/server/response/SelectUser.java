/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SelectUser {
    private SelectUser() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("USER()", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void response(ServerConnection c) {
        if (DbleServer.getInstance().isOnline()) {
            ByteBuffer buffer = c.allocate();
            buffer = HEADER.write(buffer, c, true);
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, c, true);
            }
            buffer = EOF.write(buffer, c, true);
            byte packetId = EOF.getPacketId();
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(getUser(c));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
            EOFPacket lastEof = new EOFPacket();
            lastEof.setPacketId(++packetId);
            buffer = lastEof.write(buffer, c, true);
            c.write(buffer);
        } else {
            ERROR.write(c);
        }
    }

    private static byte[] getUser(ServerConnection c) {
        return StringUtil.encode(c.getUser() + '@' + c.getHost(), c.getCharset().getResults());
    }

}
