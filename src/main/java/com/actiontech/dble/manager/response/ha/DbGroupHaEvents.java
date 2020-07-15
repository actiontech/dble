/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response.ha;

import com.actiontech.dble.backend.datasource.HaChangeStatus;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by szf on 2019/10/28.
 */
public final class DbGroupHaEvents {


    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("COMMAND", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("TRIGGER_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("STAGE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    private DbGroupHaEvents() {
    }

    public static void execute(ManagerConnection mc) {
        ByteBuffer buffer = mc.allocate();
        // write header
        buffer = HEADER.write(buffer, mc, true);
        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, mc, true);
        }
        buffer = EOF.write(buffer, mc, true);
        byte packetId = EOF.getPacketId();

        for (Map.Entry<Integer, HaChangeStatus> entry : HaConfigManager.getInstance().getUnfinised().entrySet()) {
            RowDataPacket row = getRow(entry.getValue(), mc.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, mc, true);
        }
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, mc, true);
        mc.write(buffer);
    }


    private static RowDataPacket getRow(HaChangeStatus event, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode("" + event.getIndex(), charset));
        row.add(StringUtil.encode(event.getCommand(), charset));
        row.add(StringUtil.encode(event.getType().toString(), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(event.getStartTimeStamp()), charset));
        row.add(StringUtil.encode(event.getStage().toString(), charset));
        return row;
    }
}
