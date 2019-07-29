/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SelectVersionComment {
    private SelectVersionComment() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    public static void response(FrontendConnection c) {

        byte packetId = setCurrentPacket(c);
        HEADER.setPacketId(++packetId);
        FIELDS[0] = PacketUtil.getField("@@VERSION_COMMENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(++packetId);
        EOF.setPacketId(++packetId);

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

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(Versions.VERSION_COMMENT);
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        boolean multiStatementFlag = false;
        if (c instanceof ServerConnection) {
            multiStatementFlag = ((ServerConnection) c).getSession2().getIsMultiStatement().get();
            ((ServerConnection) c).getSession2().multiStatementPacket(lastEof, packetId);
        }
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
        if (c instanceof ServerConnection) {
            ((ServerConnection) c).getSession2().multiStatementNextSql(multiStatementFlag);
        }

    }


    public static byte setCurrentPacket(FrontendConnection c) {
        if (c instanceof ServerConnection) {
            return (byte) ((ServerConnection) c).getSession2().getPacketId().get();
        }
        return 0;
    }

}
