/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.statistic.stat.QueryConditionAnalyzer;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ShowSQLCondition
 *
 * @author zhuam
 */
public final class ShowSQLCondition {
    private ShowSQLCondition() {
    }

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("KEY", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("COUNT", Fields.FIELD_TYPE_LONGLONG);
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

        String key = QueryConditionAnalyzer.getInstance().getKey();
        List<Map.Entry<Object, AtomicLong>> list = QueryConditionAnalyzer.getInstance().getValues();
        if (list != null) {

            int size = list.size();
            long total = 0L;

            for (int i = 0; i < size; i++) {
                Map.Entry<Object, AtomicLong> entry = list.get(i);
                Object value = entry.getKey();
                Long count = entry.getValue().get();
                total += count;

                RowDataPacket row = getRow(i, key, value.toString(), count, c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }

            RowDataPacket vkRow = getRow(size + 1, key + ".valuekey", "size", size, c.getCharset().getResults());
            vkRow.setPacketId(++packetId);
            buffer = vkRow.write(buffer, c, true);

            RowDataPacket vcRow = getRow(size + 2, key + ".valuecount", "total", total, c.getCharset().getResults());
            vcRow.setPacketId(++packetId);
            buffer = vcRow.write(buffer, c, true);

        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(int i, String key, String value, long count, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(i));
        row.add(StringUtil.encode(key, charset));
        row.add(StringUtil.encode(value, charset));
        row.add(LongUtil.toBytes(count));
        return row;
    }


}
