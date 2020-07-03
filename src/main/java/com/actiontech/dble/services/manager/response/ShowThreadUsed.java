/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public final class ShowThreadUsed {
    private ShowThreadUsed() {
    }
    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("THREAD_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_QUARTER_MIN", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_MINUTE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_FIVE_MINUTE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        buffer = HEADER.write(buffer, service, true);

        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        buffer = EOF.write(buffer, service, true);


        byte packetId = EOF.getPacketId();
        Map<String, ThreadWorkUsage> threadUsedMap = new TreeMap<>(DbleServer.getInstance().getThreadUsedMap());
        for (Map.Entry<String, ThreadWorkUsage> entry : threadUsedMap.entrySet()) {
            RowDataPacket row = getRow(entry.getKey(), entry.getValue(), service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(String name, ThreadWorkUsage workUsage, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        String[] workUsages = workUsage.getUsedPercent();
        row.add(StringUtil.encode(workUsages[0], charset));
        row.add(StringUtil.encode(workUsages[1], charset));
        row.add(StringUtil.encode(workUsages[2], charset));
        return row;
    }
}
