/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.statistic.stat.ThreadWorkUsage;
import com.oceanbase.obsharding_d.util.StringUtil;

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
        Map<String, ThreadWorkUsage> threadUsedMap = new TreeMap<>(OBsharding_DServer.getInstance().getThreadUsedMap());
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
