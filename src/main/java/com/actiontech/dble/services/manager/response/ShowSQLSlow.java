/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.SQLRecord;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * show slow SQL, default TOP10
 *
 * @author mycat
 * @author zhuam
 */
public final class ShowSQLSlow {
    private ShowSQLSlow() {
    }

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
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
        Map<UserName, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            UserName user = userStat.getUser();
            List<SQLRecord> keyList = userStat.getSqlRecorder().getRecords();
            for (SQLRecord key : keyList) {
                if (key != null) {
                    RowDataPacket row = getRow(user, key, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }

            if (isClear) {
                userStat.clearSqlSlow();
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(UserName user, SQLRecord sql, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(user.toString(), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(sql.getStartTime()), charset));
        row.add(LongUtil.toBytes(sql.getExecuteTime()));
        row.add(StringUtil.encode(sql.getStatement(), charset));
        return row;
    }

}
