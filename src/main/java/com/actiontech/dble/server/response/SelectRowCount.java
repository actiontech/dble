package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2020/3/18.
 */
public class SelectRowCount implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    public static void response(ShardingService service) {
        HEADER.setPacketId(service.nextPacketId());
        FIELDS[0] = PacketUtil.getField("ROW_COUNT()", Fields.FIELD_TYPE_LONG);
        FIELDS[0].setPacketId(service.nextPacketId());
        EOF.setPacketId(service.nextPacketId());

        ByteBuffer buffer = service.allocate();
        buffer = HEADER.write(buffer, service, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        buffer = EOF.write(buffer, service, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getSession2().getRowCount()));
        row.setPacketId(service.nextPacketId());
        buffer = row.write(buffer, service, true);
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());
        lastEof.write(buffer, service);
    }


    @Override
    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("ROW_COUNT()", Fields.FIELD_TYPE_LONG));
        return result;
    }


    @Override
    public List<RowDataPacket> getRows(ShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getSession2().getRowCount()));
        result.add(row);
        return result;
    }
}
