/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.IntegerUtil;

import java.nio.ByteBuffer;

/**
 * Show Active Connection
 *
 * @author mycat
 * @author mycat
 */
public final class ShowConnectionCount {
    private ShowConnectionCount() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[0] = PacketUtil.getField("CONN_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(++packetId);

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
        RowDataPacket row = getRow();
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow() {
        int cons = 0;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        for (NIOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null) {
                    cons++;
                }
            }
        }

        row.add(IntegerUtil.toBytes(cons));
        return row;
    }

}
