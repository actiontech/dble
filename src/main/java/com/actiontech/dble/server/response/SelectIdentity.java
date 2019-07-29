/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SelectIdentity {
    private SelectIdentity() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);



    public static void response(ServerConnection c, String stmt, int aliasIndex, final String orgName) {
        String alias = ParseUtil.parseAlias(stmt, aliasIndex);
        if (alias == null) {
            alias = orgName;
        }

        ByteBuffer buffer = c.allocate();

        byte packetId = setCurrentPacket(c);
        HEADER.setPacketId(++packetId);
        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields

        FieldPacket field = PacketUtil.getField(alias, orgName, Fields.FIELD_TYPE_LONGLONG);
        field.setPacketId(++packetId);
        buffer = field.write(buffer, c, true);

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, c, true);

        // write rows
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(c.getLastInsertId()));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        c.getSession2().multiStatementPacket(lastEof, packetId);
        buffer = lastEof.write(buffer, c, true);
        boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
        // post write
        c.write(buffer);
        c.getSession2().multiStatementNextSql(multiStatementFlag);
    }


    public static byte setCurrentPacket(ServerConnection c) {
        byte packetId = (byte) c.getSession2().getPacketId().get();
        return packetId;
    }

}
