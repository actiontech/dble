package io.mycat.server.response;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.server.ServerConnection;

import java.nio.ByteBuffer;


public final class InformationSchemaProfiling {
    private InformationSchemaProfiling() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    /**
     * response method.
     *
     * @param c
     */
    public static void response(ServerConnection c) {


        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;
        FIELDS[i] = PacketUtil.getField("State", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].packetId = ++packetId;
        FIELDS[i + 1] = PacketUtil.getField("Duration", Fields.FIELD_TYPE_DECIMAL);
        FIELDS[i + 1].packetId = ++packetId;

        FIELDS[i + 2] = PacketUtil.getField("Percentage", Fields.FIELD_TYPE_DECIMAL);
        FIELDS[i + 2].packetId = ++packetId;
        EOF.packetId = ++packetId;
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
        packetId = EOF.packetId;


        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);


    }


}
