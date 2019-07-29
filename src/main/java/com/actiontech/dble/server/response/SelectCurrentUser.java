/*
* Copyright (C) 2016-2019 ActionTech.
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
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectCurrentUser implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    public static void response(ServerConnection c) {
        if (DbleServer.getInstance().isOnline()) {

            byte packetId = setCurrentPacket(c);
            HEADER.setPacketId(++packetId);
            FIELDS[0] = PacketUtil.getField("CURRENT_USER()", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[0].setPacketId(++packetId);
            EOF.setPacketId(++packetId);

            ByteBuffer buffer = c.allocate();
            buffer = HEADER.write(buffer, c, true);
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, c, true);
            }
            buffer = EOF.write(buffer, c, true);

            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(getUser(c));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
            EOFPacket lastEof = new EOFPacket();
            lastEof.setPacketId(++packetId);
            c.getSession2().multiStatementPacket(lastEof, packetId);
            buffer = lastEof.write(buffer, c, true);
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            c.write(buffer);
            c.getSession2().multiStatementNextSql(multiStatementFlag);
        } else {
            ERROR.write(c);
        }
    }

    private static byte[] getUser(ServerConnection c) {
        return StringUtil.encode(c.getUser() + "@%", c.getCharset().getResults());
    }

    public static byte setCurrentPacket(ServerConnection c) {
        byte packetId = (byte) c.getSession2().getPacketId().get();
        return packetId;
    }

    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("CURRENT_USER()", Fields.FIELD_TYPE_VAR_STRING));
        return result;
    }

    public List<RowDataPacket> getRows(ServerConnection c) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(getUser(c));
        result.add(row);
        return result;
    }
}
