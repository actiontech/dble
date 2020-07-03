/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.stat.UserSqlRWStat;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * ShowSQLSumUser
 * <p>
 * 1.R/W read /write
 * 2.TIME_COUNT
 * 3.TTL_COUNT
 * 4.Net in/out bytes
 *
 * @author zhuam
 */
public final class ShowSQLSumUser {
    private ShowSQLSumUser() {
    }

    private static DecimalFormat decimalFormat = new DecimalFormat("0.00");

    private static final int FIELD_COUNT = 11;
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

        FIELDS[i] = PacketUtil.getField("R", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("W", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("R%", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("MAX", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        //22-06h, 06-13h, 13-18h, 18-22h
        FIELDS[i] = PacketUtil.getField("TIME_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        //<10ms, 10ms-200ms, 200ms-1s, >1s
        FIELDS[i] = PacketUtil.getField("TTL_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service, boolean isClear) {
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
        int i = 0;

        Map<UserName, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            i++;
            RowDataPacket row = getRow(userStat, i, service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);

        }

        if (isClear) {
            UserStatAnalyzer.getInstance().reset();
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(UserStat userStat, long idx, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        if (userStat == null) {
            row.add(StringUtil.encode(("not fond"), charset));
            return row;
        }

        UserName user = userStat.getUser();
        UserSqlRWStat rwStat = userStat.getRWStat();
        long r = rwStat.getRCount();
        long w = rwStat.getWCount();
        String rStr = decimalFormat.format(1.0D * r / (r + w));
        int max = rwStat.getConcurrentMax();

        row.add(StringUtil.encode(user.toString(), charset));
        row.add(LongUtil.toBytes(r));
        row.add(LongUtil.toBytes(w));
        row.add(StringUtil.encode(String.valueOf(rStr), charset));
        row.add(StringUtil.encode(String.valueOf(max), charset));
        row.add(LongUtil.toBytes(rwStat.getNetInBytes()));
        row.add(LongUtil.toBytes(rwStat.getNetOutBytes()));
        row.add(StringUtil.encode(rwStat.getExecuteHistogram().toString(), charset));
        row.add(StringUtil.encode(rwStat.getTimeHistogram().toString(), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(rwStat.getLastExecuteTime()), charset));

        return row;
    }

}
