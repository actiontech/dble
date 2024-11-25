/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.btrace.provider.OBsharding_DThreadPoolProvider;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.information.tables.OBsharding_DThreadPoolTask;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.NameableExecutor;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * ShowThreadPool status
 *
 * @author mycat
 * @author mycat
 */
public final class ShowThreadPoolTask {
    private ShowThreadPoolTask() {
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

        FIELDS[i] = PacketUtil.getField("ACTIVE_TASK_COUNT", Fields.FIELD_TYPE_LONG);
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

    public static void execute(ManagerService service) {
        OBsharding_DThreadPoolProvider.beginProcessShowThreadPoolTask();
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
        List<ExecutorService> executors = getExecutors();
        for (ExecutorService exec : executors) {
            if (exec != null) {
                RowDataPacket row = getRow((NameableExecutor) exec, service.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(NameableExecutor exec, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        final OBsharding_DThreadPoolTask.Row rowData = OBsharding_DThreadPoolTask.calculateRow(exec);
        row.add(StringUtil.encode(exec.getName(), charset));
        row.add(LongUtil.toBytes(rowData.getPoolSize()));
        row.add(LongUtil.toBytes(rowData.getActiveTaskCount()));
        row.add(LongUtil.toBytes(rowData.getTaskQueueSize()));
        row.add(LongUtil.toBytes(rowData.getCompletedTask()));
        row.add(LongUtil.toBytes(rowData.getTotalTaskCount()));
        return row;
    }


    private static List<ExecutorService> getExecutors() {
        List<ExecutorService> list = new LinkedList<>();
        OBsharding_DServer server = OBsharding_DServer.getInstance();
        list.add(server.getTimerExecutor());
        list.add(server.getFrontExecutor());
        list.add(server.getBackendExecutor());
        list.add(server.getComplexQueryExecutor());
        list.add(server.getWriteToBackendExecutor());
        return list;
    }

}
