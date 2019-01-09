/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SessionIsolation {
    private SessionIsolation() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    public static void response(ServerConnection c) {
        byte packetId = setCurrentPacket(c);
        HEADER.setPacketId(++packetId);
        FIELDS[0] = PacketUtil.getField("@@session.tx_isolation", Fields.FIELD_TYPE_STRING);
        FIELDS[0].setPacketId(++packetId);
        EOF.setPacketId(++packetId);

        ByteBuffer buffer = c.allocate();
        buffer = HEADER.write(buffer, c, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        buffer = EOF.write(buffer, c, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);

        String value = "";
        switch (c.getTxIsolation()) {
            case Isolations.READ_COMMITTED:
                value = "READ-COMMITTED";
                break;
            case Isolations.READ_UNCOMMITTED:
                value = "READ-UNCOMMITTED";
                break;
            case Isolations.REPEATABLE_READ:
                value = "REPEATABLE-READ";
                break;
            case Isolations.SERIALIZABLE:
                value = "SERIALIZABLE";
                break;
            default:
                break;
        }
        row.add(StringUtil.encode(value, c.getCharset().getResults()));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        c.getSession2().multiStatementPacket(lastEof, packetId);
        buffer = lastEof.write(buffer, c, true);
        boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
        c.write(buffer);
        c.getSession2().multiStatementNextSql(multiStatementFlag);
    }

    public static byte setCurrentPacket(ServerConnection c) {
        byte packetId = (byte) c.getSession2().getPacketId().get();
        return packetId;
    }

}
