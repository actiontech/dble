/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;

import java.nio.ByteBuffer;


/**
 * MysqlInformationSchemaHandler
 * <p>
 * :SELECT * FROM information_schema.CHARACTER_SETS
 *
 * @author zhuam
 */
public final class MysqlInformationSchemaHandler {
    private MysqlInformationSchemaHandler() {
    }

    /**
     * @param fieldCount
     * @param fields
     * @param c
     */
    public static void doWrite(int fieldCount, FieldPacket[] fields, ServerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        byte packetId = header.getPacketId();
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, c, true);
        }

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);

    }

    public static void handle(SchemaInfo schemaInfo, ServerConnection c) {
        if (schemaInfo != null) {
            if (schemaInfo.getTable().toUpperCase().equals("CHARACTER_SETS")) {
                int fieldCount = 4;
                FieldPacket[] fields = new FieldPacket[fieldCount];
                fields[0] = PacketUtil.getField("CHARACTER_SET_NAME", Fields.FIELD_TYPE_VAR_STRING);
                fields[1] = PacketUtil.getField("DEFAULT_COLLATE_NAME", Fields.FIELD_TYPE_VAR_STRING);
                fields[2] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
                fields[3] = PacketUtil.getField("MAXLEN", Fields.FIELD_TYPE_LONG);

                doWrite(fieldCount, fields, c);

            } else {
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            }

        } else {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        }
    }
}
