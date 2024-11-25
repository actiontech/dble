/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.FlowControllerConfig;
import com.oceanbase.obsharding_d.config.model.db.DbInstanceConfig;
import com.oceanbase.obsharding_d.config.model.db.PoolConfig;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.singleton.FlowController;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.StringUtil;

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

            Map<String, PhysicalDbGroup> dbGroups = OBsharding_DServer.getInstance().getConfig().getDbGroups();
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
