/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;

/**
 * ShowProcessor
 *
 * @author mycat
 * @author mycat
 */
public final class ShowProcessor {
    private ShowProcessor() {
    }

    private static final int FIELD_COUNT = 12;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("REACT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("R_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("W_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FREE_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TOTAL_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("BU_PERCENT", Fields.FIELD_TYPE_TINY);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("BU_WARNS", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FC_COUNT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("BC_COUNT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        byte packetId = EOF.getPacketId();
        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            RowDataPacket row = getRow(p);
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }
        for (IOProcessor p : DbleServer.getInstance().getBackendProcessors()) {
            RowDataPacket row = getRow(p);
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }
        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(IOProcessor processor) {
        BufferPool bufferPool = processor.getBufferPool();
        long bufferSize = bufferPool.size();
        long bufferCapacity = bufferPool.capacity();
        long bufferSharedOpts = bufferPool.getSharedOptsCount();
        long bufferUsagePercent = (bufferCapacity - bufferSize) * 100 / bufferCapacity;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(processor.getName().getBytes());
        row.add(LongUtil.toBytes(processor.getNetInBytes()));
        row.add(LongUtil.toBytes(processor.getNetOutBytes()));
        row.add(LongUtil.toBytes(0));
        row.add(IntegerUtil.toBytes(0));
        row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
        row.add(LongUtil.toBytes(bufferSize));
        row.add(LongUtil.toBytes(bufferCapacity));
        row.add(LongUtil.toBytes(bufferUsagePercent));
        row.add(LongUtil.toBytes(bufferSharedOpts));
        row.add(IntegerUtil.toBytes(processor.getFrontends().size()));
        row.add(IntegerUtil.toBytes(processor.getBackends().size()));
        return row;
    }

}
