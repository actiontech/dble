/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.stat.QueryTimeCost;
import com.actiontech.dble.statistic.stat.QueryTimeCostContainer;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * ShowBackend
 *
 * @author mycat
 */
public final class ShowCostTimeStat {
    private ShowCostTimeStat() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("OVER_ALL(µs)", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("FRONT_PREPARE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("BACKEND_EXECUTE", Fields.FIELD_TYPE_VAR_STRING);
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
        QueryTimeCost[] recorders = QueryTimeCostContainer.getInstance().getRecorders();
        int realPos = QueryTimeCostContainer.getInstance().getRealPos();
        int start = 0;
        int end = realPos;
        if (realPos != recorders.length - 1 && recorders[realPos + 1] != null) {
            start = realPos + 1;
            end = realPos + recorders.length;
        }
        for (int i = start; i <= end; i++) {
            RowDataPacket row = getRow(recorders[i % recorders.length], service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(QueryTimeCost queryTimeCost, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes((queryTimeCost.getResponseTime().get() - queryTimeCost.getRequestTime()) / 1000));
        String[] backendInfo = getBackendConnCost(queryTimeCost);
        row.add(StringUtil.encode(backendInfo[0], charset));
        row.add(StringUtil.encode(backendInfo[1], charset));
        return row;
    }

    private static String[] getBackendConnCost(QueryTimeCost queryTimeCost) {
        int i = 0;
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (Map.Entry<Long, QueryTimeCost> backCostEntry : queryTimeCost.getBackEndTimeCosts().entrySet()) {
            if (i != 0) {
                sb.append(";");
                sb2.append(";");
            }
            long id = backCostEntry.getKey();
            sb.append("Id:");
            sb.append(id);
            sb.append(",Time:");
            QueryTimeCost backendCost = backCostEntry.getValue();
            sb.append((backendCost.getRequestTime() - queryTimeCost.getRequestTime()) / 1000);

            sb2.append("Id:");
            sb2.append(id);
            sb2.append(",Time:");
            sb2.append((backendCost.getResponseTime().get() - backendCost.getRequestTime()) / 1000);
            i++;
        }
        return new String[]{sb.toString(), sb2.toString()};
    }
}
