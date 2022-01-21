/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.FlowController;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlShow {

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("FLOW_CONTROL_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FLOW_CONTROL_HIGH_LEVEL", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FLOW_CONTROL_LOW_LEVEL", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private FlowControlShow() {

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

        FlowControllerConfig config = FlowController.getFlowControllerConfig();
        if (config.isEnableFlowControl()) {
            //find
            {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("FRONT_END", service.getCharset().getResults()));
                row.add(LongUtil.toBytes(config.getHighWaterLevel()));
                row.add(LongUtil.toBytes(config.getLowWaterLevel()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }

            Map<String, PhysicalDbGroup> dbGroups = DbleServer.getInstance().getConfig().getDbGroups();
            for (PhysicalDbGroup dbGroup : dbGroups.values()) {
                for (PhysicalDbInstance dbInstance : dbGroup.getDbInstances(true)) {
                    DbInstanceConfig dbInstanceConfig = dbInstance.getConfig();
                    PoolConfig poolConfig = dbInstanceConfig.getPoolConfig();
                    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode(dbGroup.getGroupName() + "-" + dbInstance.getName(), service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(poolConfig.getFlowHighLevel()));
                    row.add(LongUtil.toBytes(poolConfig.getFlowLowLevel()));
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }
        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }
}
