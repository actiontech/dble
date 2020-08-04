/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2018/8/16.
 */
public final class ShowPauseInfo {

    private ShowPauseInfo() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PAUSE_SHARDING_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }


    public static void execute(ManagerService service) {
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
        if (PauseShardingNodeManager.getInstance().getShardingNodes() != null) {
            for (String shardingNode : PauseShardingNodeManager.getInstance().getShardingNodes()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.setPacketId(++packetId);
                row.add(StringUtil.encode(shardingNode, service.getCharset().getResults()));
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }


}
