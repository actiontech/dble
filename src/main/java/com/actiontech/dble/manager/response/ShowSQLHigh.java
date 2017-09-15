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
import com.actiontech.dble.statistic.stat.SqlFrequency;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * ShowSQLHigh
 *
 * @author zhuam
 */
public final class ShowSQLHigh {
    private ShowSQLHigh() {
    }

    private static final int FIELD_COUNT = 9;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FREQUENCY", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("AVG_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MIN_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, boolean isClear) {
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

        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            String user = userStat.getUser();
            List<SqlFrequency> list = userStat.getSqlHigh().getSqlFrequency(isClear);
            if (list != null) {
                int i = 1;
                for (SqlFrequency sqlFrequency : list) {
                    if (sqlFrequency != null) {
                        RowDataPacket row = getRow(i, user, sqlFrequency.getSql(), sqlFrequency.getCount(),
                                sqlFrequency.getAvgTime(), sqlFrequency.getMaxTime(), sqlFrequency.getMinTime(),
                                sqlFrequency.getExecuteTime(), sqlFrequency.getLastTime(), c.getCharset().getResults());
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, c, true);
                        i++;
                    }
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(int i, String user, String sql, long count, long avgTime, long maxTime,
                                        long minTime, long executTime, long lastTime, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(i));
        row.add(StringUtil.encode(user, charset));
        row.add(LongUtil.toBytes(count));
        row.add(LongUtil.toBytes(avgTime));
        row.add(LongUtil.toBytes(maxTime));
        row.add(LongUtil.toBytes(minTime));
        row.add(LongUtil.toBytes(executTime));
        row.add(StringUtil.encode(FormatUtil.formatDate(lastTime), charset));
        row.add(StringUtil.encode(sql, charset));
        return row;
    }


}
