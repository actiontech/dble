/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.NameableExecutor;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * ShowThreadPool status
 *
 * @author mycat
 * @author mycat
 */
public final class ShowThreadPool {
    private ShowThreadPool() {
    }

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("POOL_SIZE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TASK_QUEUE_SIZE",
                Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("COMPLETED_TASK",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TOTAL_TASK",
                Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

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
        List<NameableExecutor> executors = getExecutors();
        for (NameableExecutor exec : executors) {
            if (exec != null) {
                RowDataPacket row = getRow(exec, c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(NameableExecutor exec, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(exec.getName(), charset));
        row.add(IntegerUtil.toBytes(exec.getPoolSize()));
        row.add(IntegerUtil.toBytes(exec.getActiveCount()));
        row.add(IntegerUtil.toBytes(exec.getQueue().size()));
        row.add(LongUtil.toBytes(exec.getCompletedTaskCount()));
        row.add(LongUtil.toBytes(exec.getTaskCount()));
        return row;
    }

    private static List<NameableExecutor> getExecutors() {
        List<NameableExecutor> list = new LinkedList<>();
        DbleServer server = DbleServer.getInstance();
        list.add(server.getTimerExecutor());
        list.add(server.getBusinessExecutor());
        list.add(server.getComplexQueryExecutor());
        // for (NIOProcessor pros : server.getProcessors()) {
        // list.add(pros.getExecutor());
        // }
        return list;
    }
}
